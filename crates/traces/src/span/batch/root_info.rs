use super::{Span, Time, TimeExt, service_name};

/// Compute the trace-level columns for one trace: the root service and name,
/// the trace start, and the trace duration.
///
/// CONTRACT: `spans` must be the COMPLETE per-trace span set. A time-windowed
/// or otherwise filtered subset yields trace-level values that reflect only the
/// subset, not the trace. Callers that materialize rows from a clipped window
/// must use [`span_batch_for_window`] and pass the full trace's spans here.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RootInfo {
    pub(crate) service_name: String,
    pub(crate) span_name: String,
    pub(crate) start_ns: i64,
    pub(crate) duration: Time,
}

pub(crate) fn root_info(spans: &[Span]) -> RootInfo {
    let root = spans
        .iter()
        .find(|span| span.is_root())
        .or_else(|| spans.iter().min_by_key(|span| span.start_ns));
    let service = root
        .and_then(|span| service_name(&span.resource_attrs))
        .unwrap_or_default();
    let name = root.map(|span| span.name.clone()).unwrap_or_default();
    let bounds = crate::trace_time_bounds(spans.iter().map(|span| crate::SpanTiming {
        start_ns: span.start_ns,
        duration_ns: span.duration_ns,
    }));
    RootInfo {
        service_name: service,
        span_name: name,
        start_ns: bounds.start_ns,
        duration: Time::from_nanos(bounds.end_ns.saturating_sub(bounds.start_ns)),
    }
}
