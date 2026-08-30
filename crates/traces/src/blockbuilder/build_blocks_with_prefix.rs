use super::*;

/// Build and write one span block, with an object-store prefix on its key.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub async fn build_blocks_with_prefix(
    writer: &BlockWriter,
    index: &mut TraceIndex,
    object_key_prefix: &str,
    tenant: &str,
    partition: i32,
    records: &[SpanRecord],
    offset_range: (i64, i64),
) -> Result<Vec<BlockMeta>, TracesError> {
    build_blocks_with_options(
        writer,
        index,
        tenant,
        partition,
        records,
        offset_range,
        BlockBuildOptions {
            object_key_prefix,
            promoted_attrs: &[],
        },
    )
    .await
}
