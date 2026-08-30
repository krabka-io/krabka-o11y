use super::{Arc, ObjectStore, BlockWriter, TraceIndex, BlockMeta, TracesError, compact_block_keys_with_max_bytes, DEFAULT_BLOCK_READ_MAX};

/// Merge existing span block object keys into one replacement block and one
/// index entry.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub async fn compact_block_keys(
    store: Arc<dyn ObjectStore>,
    writer: &BlockWriter,
    index: &mut TraceIndex,
    tenant: &str,
    input_keys: &[String],
    output_key: &str,
) -> Result<BlockMeta, TracesError> {
    compact_block_keys_with_max_bytes(
        store,
        writer,
        index,
        tenant,
        input_keys,
        output_key,
        DEFAULT_BLOCK_READ_MAX,
    )
    .await
}
