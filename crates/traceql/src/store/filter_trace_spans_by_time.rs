use super::TraceSpans;

#[must_use]
pub fn filter_trace_spans_by_time(mut trace: TraceSpans, start_ns: i64, end_ns: i64) -> TraceSpans {
    trace.spans.retain(|span| {
        let Ok(start) = i64::try_from(span.start_time_unix_nano) else {
            return false;
        };
        start >= start_ns && start <= end_ns
    });
    trace
}
