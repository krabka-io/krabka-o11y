use super::{SpanSet, TraceJson, TraceResult, parse_hex16};

impl From<&TraceJson> for TraceResult {
    fn from(t: &TraceJson) -> Self {
        TraceResult {
            trace_id: parse_hex16(&t.trace_id),
            root_service_name: t.root_service_name.clone(),
            root_trace_name: t.root_trace_name.clone(),
            start_time_unix_nano: t.start_time_unix_nano.parse().unwrap_or(0),
            duration: t.duration,
            span_sets: t.span_sets.iter().map(SpanSet::from).collect(),
        }
    }
}
