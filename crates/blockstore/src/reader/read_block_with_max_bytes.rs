use super::{
    Arc, ByteSize, ObjectStore, ObjectStoreReader, ParquetRecordBatchStreamBuilder, Path,
    RecordBatch, Result, TryStreamExt, head_within_cap, instrument, unreadable,
};

/// Reads every `RecordBatch` with a caller-supplied on-disk size limit.
///
/// # Errors
/// Returns an error when object-store I/O fails, the block exceeds
/// `max_bytes`, persisted metadata is malformed, or the block cannot be
/// decoded.
#[instrument(
    level = "debug",
    skip_all,
    fields(object_key = %object_key, size = tracing::field::Empty),
    err
)]
pub async fn read_block_with_max_bytes(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
    max_bytes: ByteSize,
) -> Result<Vec<RecordBatch>> {
    let path = Path::from(object_key);
    head_within_cap(&store, &path, object_key, max_bytes).await?;
    let reader = ObjectStoreReader::new(store, path);
    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|error| unreadable(object_key, error))?;
    crate::validate_persisted_block_format(builder.metadata())
        .map_err(crate::BlockStoreError::InvalidBlock)?;
    let stream = builder
        .build()
        .map_err(|error| unreadable(object_key, error))?;
    stream
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| unreadable(object_key, error))
}
