use super::*;

pub(crate) struct StoredTrace {
    pub(crate) trace_id: [u8; 16],
    pub(crate) root_service_name: String,
    pub(crate) root_span_name: String,
    pub(crate) trace_start_unix_nano: i64,
    pub(crate) trace_duration: Time,
    pub(crate) spans: Vec<InputSpan>,
    pub(crate) nested: Vec<NestedSet>,
}
