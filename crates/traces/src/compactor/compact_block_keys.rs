use super::{
    Arc, BlockMeta, BlockWriter, DEFAULT_BLOCK_READ_MAX, ObjectStore, TraceIndex, TracesError,
    compact_block_keys_with_max_bytes,
};

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
