use super::{
    Arc, ByteSize, ByteSizeExt, ObjectStore, ObjectStoreReader, ParquetRecordBatchStreamBuilder,
    Path, Result, RowGroupMeta, head_within_cap, instrument,
};

/// Reads row-group sizes with a caller-supplied on-disk size limit.
///
/// # Errors
/// Returns an error when object-store I/O fails, the block exceeds
/// `max_bytes`, or persisted metadata is malformed.
#[instrument(
    level = "debug",
    skip_all,
    fields(object_key = %object_key, size = tracing::field::Empty),
    err
)]
pub async fn read_row_group_metadata_with_max_bytes(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
    max_bytes: ByteSize,
) -> Result<Vec<RowGroupMeta>> {
    let path = Path::from(object_key);
    head_within_cap(&store, &path, object_key, max_bytes).await?;
    let reader = ObjectStoreReader::new(store, path);
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    Ok(builder
        .metadata()
        .row_groups()
        .iter()
        .enumerate()
        .map(|(index, row_group)| RowGroupMeta {
            index,
            compressed: ByteSize::from_bytes(
                u64::try_from(row_group.compressed_size()).unwrap_or(0),
            ),
        })
        .collect())
}
