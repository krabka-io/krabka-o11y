use super::{
    Arc, DEFAULT_BLOCK_READ_MAX, ObjectStore, Result, RowGroupMeta,
    read_row_group_metadata_with_max_bytes,
};

/// Reads row-group sizes from Parquet metadata and does not scan row data.
///
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_row_group_metadata(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
) -> Result<Vec<RowGroupMeta>> {
    read_row_group_metadata_with_max_bytes(store, object_key, DEFAULT_BLOCK_READ_MAX).await
}
