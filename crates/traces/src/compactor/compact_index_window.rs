use super::{Arc, BlockMeta, BlockWriter, DEFAULT_BLOCK_READ_MAX, ObjectStore, TraceIndex, TracesError, compact_index_window_with_max_bytes};

/// Compact every tenant in the selected time window independently.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub async fn compact_index_window(
    store: Arc<dyn ObjectStore>,
    writer: &BlockWriter,
    index: &mut TraceIndex,
    object_key_prefix: &str,
    start_ns: i64,
    end_ns: i64,
) -> Result<Vec<BlockMeta>, TracesError> {
    compact_index_window_with_max_bytes(
        store,
        writer,
        index,
        object_key_prefix,
        start_ns,
        end_ns,
        DEFAULT_BLOCK_READ_MAX,
    )
    .await
}
