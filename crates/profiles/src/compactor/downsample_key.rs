use super::*;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DownsampleKey {
    pub(crate) series_fingerprint: u64,
    pub(crate) timestamp: i64,
    pub(crate) profile_type: String,
    pub(crate) stacktrace_id: u64,
    pub(crate) stacktrace_partition: u64,
    pub(crate) span_id: Option<u64>,
    pub(crate) trace_id: Option<Vec<u8>>,
}
