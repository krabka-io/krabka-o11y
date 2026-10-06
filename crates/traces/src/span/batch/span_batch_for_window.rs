use super::{
    PromotedSpanAttr, RecordBatch, Span, TracesError, encode_span_rows_with_promoted_attrs,
    span_rows_for_window,
};

/// Build one span-block `RecordBatch` whose rows are `row_spans` but whose
/// trace-level columns are computed over `trace_spans`.
///
/// Use this when a query window clips a trace. `row_spans` is the in-window
/// subset. `trace_spans` is the trace's full span set, so that
/// `root_service_name`, `root_span_name`, `trace_start_unix_nano` and
/// `trace_duration_nanos` reflect the whole trace rather than only the window.
/// Pass the same slice for both to materialize a complete trace.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn span_batch_for_window(
    row_spans: &[Span],
    trace_spans: &[Span],
    promoted_attrs: &[PromotedSpanAttr],
) -> Result<RecordBatch, TracesError> {
    let rows = span_rows_for_window(row_spans, trace_spans);

    encode_span_rows_with_promoted_attrs(&rows, promoted_attrs)
        .map_err(|err| TracesError::Block(err.to_string()))
}
