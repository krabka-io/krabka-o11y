use super::{BlockWriter, TraceIndex, SpanRecord, PromotedSpanAttr, BlockMeta, TracesError, build_blocks_with_options, BlockBuildOptions};

///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub async fn build_blocks_with_promoted_attrs(
    writer: &BlockWriter,
    index: &mut TraceIndex,
    tenant: &str,
    partition: i32,
    records: &[SpanRecord],
    offset_range: (i64, i64),
    promoted_attrs: &[PromotedSpanAttr],
) -> Result<Vec<BlockMeta>, TracesError> {
    build_blocks_with_options(
        writer,
        index,
        tenant,
        partition,
        records,
        offset_range,
        BlockBuildOptions {
            object_key_prefix: "",
            promoted_attrs,
        },
    )
    .await
}
