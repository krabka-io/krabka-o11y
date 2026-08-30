use super::{
    Arc, DEFAULT_BLOCK_READ_MAX, ObjectStore, RecordBatch, Result,
    read_block_row_groups_with_max_bytes,
};

/// Reads selected row groups from a Parquet block.
///
/// As with [`read_block`], the reader rejects the block when its on-disk size
/// exceeds [`DEFAULT_BLOCK_READ_MAX`], before it streams any bytes.
///
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_block_row_groups(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
    row_groups: &[usize],
) -> Result<Vec<RecordBatch>> {
    read_block_row_groups_with_max_bytes(store, object_key, row_groups, DEFAULT_BLOCK_READ_MAX)
        .await
}
