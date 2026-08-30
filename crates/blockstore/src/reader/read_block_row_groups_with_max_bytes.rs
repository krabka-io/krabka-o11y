use super::{
    Arc, ByteSize, ObjectStore, ObjectStoreReader, ParquetRecordBatchStreamBuilder, Path,
    RecordBatch, Result, TryStreamExt, head_within_cap, instrument,
};

/// Reads selected row groups with a caller-supplied on-disk size limit.
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
pub async fn read_block_row_groups_with_max_bytes(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
    row_groups: &[usize],
    max_bytes: ByteSize,
) -> Result<Vec<RecordBatch>> {
    let path = Path::from(object_key);
    head_within_cap(&store, &path, object_key, max_bytes).await?;
    let reader = ObjectStoreReader::new(store, path);
    let stream = ParquetRecordBatchStreamBuilder::new(reader)
        .await?
        .with_row_groups(row_groups.to_vec())
        .build()?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    Ok(batches)
}
