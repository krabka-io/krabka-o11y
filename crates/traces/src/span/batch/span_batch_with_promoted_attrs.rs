use super::{PromotedSpanAttr, RecordBatch, Span, TracesError, span_batch_for_window};

/// Build one span-block `RecordBatch` from spans of one trace with configured
/// attributes duplicated into dedicated columns.
///
/// `spans` must be the complete per-trace span set. The trace-level columns,
/// which are the root service and name, the start and the duration, are
/// computed over exactly these spans.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn span_batch_with_promoted_attrs(
    spans: &[Span],
    promoted_attrs: &[PromotedSpanAttr],
) -> Result<RecordBatch, TracesError> {
    span_batch_for_window(spans, spans, promoted_attrs)
}
