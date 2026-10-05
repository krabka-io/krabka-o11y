use std::borrow::Borrow;

use super::{
    BlockNestedSet, Span, SpanRow, Time, TimeExt, assign_nested_set, block_kind, block_status,
    child_counts, root_info, span_attrs, span_events, span_links,
};

/// Build rows for one trace, keeping whole-trace metadata when its window is clipped.
/// Callers can then pack several traces without mixing their root columns.
pub(crate) fn span_rows_for_window(
    row_spans: &[impl Borrow<Span>],
    trace_spans: &[Span],
) -> Vec<SpanRow> {
    // Nested-set intervals and child counts describe the rows themselves, so
    // they are computed over `row_spans`. Trace-level columns describe the
    // whole trace, so they come from `trace_spans`.
    let nested = assign_nested_set(row_spans);
    let child_counts = child_counts(&nested);
    let (root_service_name, root_span_name, trace_start, trace_duration) = root_info(trace_spans);
    row_spans
        .iter()
        .map(Borrow::borrow)
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
        .collect()
}
