use super::{
    Arc, BlockMeta, BlockWriter, CompactionPolicy, DEFAULT_BLOCK_READ_MAX, ObjectStore, TraceIndex,
    TracesError, compact_once_with_policy,
};

/// Runs one compaction pass with the default on-disk block-read limit.
///
/// # Errors
/// Returns an error when an input exceeds the default cap, an input block is
/// malformed, or the backing span store fails.
pub async fn compact_once(
    store: Arc<dyn ObjectStore>,
    writer: &BlockWriter,
    index: &mut TraceIndex,
    object_key_prefix: &str,
    policy: CompactionPolicy,
) -> Result<Vec<BlockMeta>, TracesError> {
    compact_once_with_policy(
        store,
        writer,
        index,
        object_key_prefix,
        policy,
        DEFAULT_BLOCK_READ_MAX,
    )
    .await
}
