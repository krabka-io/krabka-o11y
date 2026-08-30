use super::*;

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
    // Nested-set intervals and child counts describe the rows themselves, so
    // they are computed over `row_spans`. Trace-level columns describe the
    // whole trace, so they come from `trace_spans`.
    let nested = assign_nested_set(row_spans);
    let child_counts = child_counts(&nested);
    let (root_service_name, root_span_name, trace_start, trace_duration) = root_info(trace_spans);
    let spans = row_spans;
    let rows = spans
        .iter()
        .zip(nested)
        .zip(child_counts)
        .map(|((span, nested_set), child_count)| SpanRow {
            trace_id: span.trace_id,
            span_id: span.span_id,
            parent_span_id: span.parent_span_id,
            nested_set: BlockNestedSet {
                nested_set_left: nested_set.left,
                nested_set_right: nested_set.right,
                parent_id: nested_set.parent_id,
            },
            child_count,
            root_service_name: Some(root_service_name.clone()),
            root_span_name: Some(root_span_name.clone()),
            trace_start_unix_nano: trace_start,
            trace_duration,
            name: Some(span.name.clone()),
            kind: block_kind(span.kind),
            start_unix_nano: span.start_ns,
            duration: Time::from_nanos(span.duration_ns),
            status_code: block_status(span.status),
            status_message: Some(span.status_message.clone()),
            instrumentation_name: Some(span.instrumentation_scope.clone()),
            instrumentation_version: Some(span.instrumentation_version.clone()),
            attrs: span_attrs(span),
            events: span_events(span),
            links: span_links(span),
        })
        .collect::<Vec<_>>();

    encode_span_rows_with_promoted_attrs(&rows, promoted_attrs)
        .map_err(|err| TracesError::Block(err.to_string()))
}
