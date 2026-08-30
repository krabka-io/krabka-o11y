#[derive(Clone, Debug)]
pub(crate) struct SampleRow {
    pub(crate) profile_type: String,
    pub(crate) fingerprint: u64,
    pub(crate) labels: Vec<(String, String)>,
    pub(crate) partition: u64,
    pub(crate) stacktrace_id: u32,
    pub(crate) value: i64,
    pub(crate) total_value: i64,
    pub(crate) span_id: Option<u64>,
    pub(crate) trace_id: Option<Vec<u8>>,
    pub(crate) timestamp_ms: i64,
}
