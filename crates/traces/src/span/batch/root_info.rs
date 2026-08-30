use super::{Span, Time, service_name, TimeExt};

/// Compute the trace-level columns for one trace: the root service and name,
/// the trace start, and the trace duration.
///
/// CONTRACT: `spans` must be the COMPLETE per-trace span set. A time-windowed
/// or otherwise filtered subset yields trace-level values that reflect only the
/// subset, not the trace. Callers that materialize rows from a clipped window
/// must use [`span_batch_for_window`] and pass the full trace's spans here.
pub(crate) fn root_info(spans: &[Span]) -> (String, String, i64, Time) {
    let root = spans
        .iter()
        .find(|span| span.is_root())
        .or_else(|| spans.iter().min_by_key(|span| span.start_ns));
    let service = root
        .and_then(|span| service_name(&span.resource_attrs))
        .unwrap_or_default();
    let name = root.map(|span| span.name.clone()).unwrap_or_default();
    let start = spans.iter().map(|span| span.start_ns).min().unwrap_or(0);
    let end = spans
        .iter()
        .map(|span| span.start_ns.saturating_add(span.duration_ns))
        .max()
        .unwrap_or(start);
    (
        service,
        name,
        start,
        Time::from_nanos(end.saturating_sub(start)),
    )
}
