use super::{SpanSet, Time};

/// One trace in a search response.
#[derive(Clone, Debug, PartialEq)]
pub struct TraceResult {
    pub trace_id: [u8; 16],
    pub root_service_name: String,
    pub root_trace_name: String,
    pub start_time_unix_nano: u64,
    /// How long the trace ran, from the earliest span start to the latest span
    /// end. The Tempo search JSON shows this duration twice, as `durationMs`
    /// and as the span-set `durationNanos`. Both come from this one field.
    pub duration: Time,
    pub span_sets: Vec<SpanSet>,
}
