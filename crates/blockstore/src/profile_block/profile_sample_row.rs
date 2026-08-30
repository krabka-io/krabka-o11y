/// One flattened profile sample row.
#[derive(Clone, Debug, PartialEq)]
pub struct ProfileSampleRow {
    pub series_fingerprint: u64,
    pub timestamp: i64,
    pub profile_type: String,
    pub stacktrace_id: u64,
    pub value: i64,
    pub stacktrace_partition: u64,
    pub total_value: i64,
    pub span_id: Option<u64>,
    pub trace_id: Option<Vec<u8>>,
}
