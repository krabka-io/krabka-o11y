/// One sample's raw payload, still unsymbolized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedSample {
    pub stacktrace_location_refs: Vec<u32>,
    pub value: i64,
    pub timestamp_ns: i64,
    pub span_id: Option<u64>,
    pub trace_id: Option<Vec<u8>>,
}
