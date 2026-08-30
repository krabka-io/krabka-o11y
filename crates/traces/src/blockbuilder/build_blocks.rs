use super::*;

/// Build and write one span block for `tenant` from the supplied WAL records.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub async fn build_blocks(
    writer: &BlockWriter,
    index: &mut TraceIndex,
    tenant: &str,
    partition: i32,
    records: &[SpanRecord],
    offset_range: (i64, i64),
) -> Result<Vec<BlockMeta>, TracesError> {
    build_blocks_with_promoted_attrs(writer, index, tenant, partition, records, offset_range, &[])
        .await
}
