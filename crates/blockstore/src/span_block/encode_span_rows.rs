use super::{RecordBatch, Result, SpanRow, encode_span_rows_with_promoted_attrs};

/// Encodes rows into a record batch that matches the canonical span-block
/// schema.
///
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn encode_span_rows(rows: &[SpanRow]) -> Result<RecordBatch> {
    encode_span_rows_with_promoted_attrs(rows, &[])
}
