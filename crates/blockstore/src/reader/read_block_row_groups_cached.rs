use super::{
    Arc, BlockMetadataCache, ByteSize, CachedBlock, ObjectStore, ObjectStoreReader,
    ParquetRecordBatchStreamBuilder, Path, RecordBatch, Result, TryStreamExt, head_within_cap,
    instrument, unreadable,
};

/// Reads selected row groups, reading the block's footer through `cache` when
/// one is supplied.
///
/// # Errors
/// Returns an error when object-store I/O fails, the block exceeds
/// `max_bytes`, persisted metadata is malformed, or the block cannot be
/// decoded.
#[instrument(
    level = "debug",
    skip_all,
    fields(object_key = %object_key, row_groups = row_groups.len(), size = tracing::field::Empty),
    err
)]
pub(crate) async fn read_block_row_groups_cached(
    store: &Arc<dyn ObjectStore>,
    object_key: &str,
    row_groups: &[usize],
    max_bytes: ByteSize,
    cache: Option<&BlockMetadataCache>,
) -> Result<Vec<RecordBatch>> {
    let path = Path::from(object_key);
    let meta = head_within_cap(store, &path, object_key, max_bytes).await?;
    let reader = match cache {
        Some(cache) => ObjectStoreReader::with_cache(
            Arc::clone(store),
            CachedBlock {
                meta,
                cache: cache.clone(),
            },
        ),
        None => ObjectStoreReader::new(Arc::clone(store), path),
    };
    let stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|error| unreadable(object_key, error))?
        .with_row_groups(row_groups.to_vec())
        .build()
        .map_err(|error| unreadable(object_key, error))?;
    stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| unreadable(object_key, error))
}
