use super::*;

pub(crate) fn trace_duration(trace: &TraceSpans) -> Option<Time> {
    let start = trace
        .spans
        .iter()
        .map(|span| span.start_time_unix_nano)
        .min()?;
    let end = trace.spans.iter().map(span_end_unix_nano).max()?;
    Some(Time::from_nanos(
        i64::try_from(end.saturating_sub(start)).unwrap_or(i64::MAX),
    ))
}
