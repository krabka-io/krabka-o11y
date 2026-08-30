use super::{Labels, PprofProfile};

/// One decoded pprof plus its series labels, before the multi-value split.
#[derive(Debug, Clone)]
pub struct RawProfile {
    pub labels: Labels,
    pub profile: PprofProfile,
    pub delta: bool,
    pub sample_timestamps_ns: Vec<Vec<i64>>,
    pub sample_span_ids: Vec<Option<u64>>,
    pub sample_trace_ids: Vec<Option<Vec<u8>>>,
}
