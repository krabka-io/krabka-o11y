use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuiltSample {
    pub series_fingerprint: u64,
    pub timestamp_ns: i64,
    pub profile_type: String,
    pub stacktrace_id: u64,
    pub value: i64,
    pub stacktrace_partition: u64,
    pub total_value: i64,
    pub span_id: Option<u64>,
    pub trace_id: Option<Vec<u8>>,
}
