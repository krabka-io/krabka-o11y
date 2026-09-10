use super::{Arc, ByteSize, ObjectStore, RecordBatch, Result, read_block_row_groups_cached};

/// Reads selected row groups with a caller-supplied on-disk size limit.
///
/// Reads through no cache; see
/// [`read_row_group_metadata_with_max_bytes`](super::read_row_group_metadata_with_max_bytes).
///
/// # Errors
/// Returns an error when object-store I/O fails, the block exceeds
/// `max_bytes`, persisted metadata is malformed, or the block cannot be
/// decoded.
pub async fn read_block_row_groups_with_max_bytes(
    store: Arc<dyn ObjectStore>,
    object_key: &str,
    row_groups: &[usize],
    max_bytes: ByteSize,
) -> Result<Vec<RecordBatch>> {
    read_block_row_groups_cached(&store, object_key, row_groups, max_bytes, None).await
}
