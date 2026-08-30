use super::{SpanRef, Time};

pub(crate) struct TraceAcc {
    pub(crate) root_service_name: String,
    pub(crate) root_trace_name: String,
    pub(crate) start_time_unix_nano: u64,
    pub(crate) duration: Time,
    pub(crate) spans: Vec<SpanRef>,
}
