use super::{Arc, ByteSize, ObjectStore, Result, RowGroupMeta, instrument, row_group_metadata};

/// Reads row-group sizes with a caller-supplied on-disk size limit.
///
/// Reads through no cache. A [`BlockStore`](crate::BlockStore) has one and uses
/// it; this free function has nothing to hang one on.
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
    row_group_metadata(&store, object_key, max_bytes, None).await
}
