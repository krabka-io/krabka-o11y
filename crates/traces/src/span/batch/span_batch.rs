use super::{RecordBatch, Span, TracesError, span_batch_with_promoted_attrs};

/// Build one span-block `RecordBatch` from spans of one trace.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn span_batch(spans: &[Span]) -> Result<RecordBatch, TracesError> {
    span_batch_with_promoted_attrs(spans, &[])
}
