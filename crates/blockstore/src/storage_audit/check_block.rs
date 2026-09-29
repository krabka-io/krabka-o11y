use super::{
    Arc, BlockStoreError, DEFAULT_BLOCK_READ_MAX, ListedObject, MERGE_READ_BATCH_ROWS, ObjectStore,
    StorageAuditError, StorageFinding, StorageFindingKind, StreamExt, block_metadata,
    open_block_stream,
};

const UNSUPPORTED_FORMAT: &str = "unsupported persisted block format version";

/// Reads the Parquet footer of one block, and with `verify_data` every row
/// batch as well.
///
/// Returns the finding for a block that does not decode. A block that
/// vanished since the listing yields nothing: a concurrent retention pass
/// does that, and it is not damage.
///
/// # Errors
/// Returns [`StorageAuditError::ObjectStore`] when the store fails for a
/// reason other than a missing object. Such a failure says nothing about the
/// block, so the audit stops rather than call the block corrupt.
pub async fn check_block(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    listed: &ListedObject,
    verify_data: bool,
) -> Result<Option<StorageFinding>, StorageAuditError> {
    let outcome = match block_metadata(store, key, DEFAULT_BLOCK_READ_MAX, None).await {
        Ok(_) if verify_data => drain_block(store, key).await,
        Ok(_) => Ok(()),
        Err(error) => Err(error),
    };
    let Err(error) = outcome else {
        return Ok(None);
    };
    if error.is_block_missing() {
        return Ok(None);
    }
    let kind = match &error {
        BlockStoreError::InvalidBlock(message) if message.contains(UNSUPPORTED_FORMAT) => {
            StorageFindingKind::UnsupportedFormat
        }
        BlockStoreError::BlockUnreadable { .. }
            if error
                .unreadable_block()
                .and_then(|(_, failure)| failure.skip_reason())
                .is_none() =>
        {
            return Err(StorageAuditError::ObjectStore(error.to_string()));
        }
        BlockStoreError::BlockUnreadable { .. }
        | BlockStoreError::InvalidBlock(_)
        | BlockStoreError::Parquet(_)
        | BlockStoreError::DataFusion(_) => StorageFindingKind::CorruptBlock,
        _ => return Err(StorageAuditError::BlockStore(error.to_string())),
    };
    Ok(Some(StorageFinding::new(
        kind,
        listed.object.signal,
        listed.object.tenant.clone(),
        key,
        error.to_string(),
    )))
}

async fn drain_block(store: &Arc<dyn ObjectStore>, key: &str) -> Result<(), BlockStoreError> {
    let (_, _, mut batches) = open_block_stream(
        Arc::clone(store),
        key,
        DEFAULT_BLOCK_READ_MAX,
        MERGE_READ_BATCH_ROWS,
    )
    .await?;
    while let Some(batch) = batches.next().await {
        batch?;
    }
    Ok(())
}
