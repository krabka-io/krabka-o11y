use super::{Frequency, ByteSize, FrequencyExt, kibibytes, Limits, u64_limit_from_usize};

/// Per-tenant distributor limits enforced before WAL append.
#[derive(Clone, Debug)]
pub struct TenantLimits {
    pub max_spans_per_request: usize,
    pub max_spans_per_trace: usize,
    /// Sustained ingest rate ceiling. Zero is unlimited.
    pub max_ingest_rate: Frequency,
    pub ingest_rate_burst: usize,
    /// Maximum size of one attribute key plus its value.
    pub max_attr_value: ByteSize,
}

impl Default for TenantLimits {
    fn default() -> Self {
        Self {
            max_spans_per_request: 10_000,
            max_spans_per_trace: usize::MAX,
            max_ingest_rate: <Frequency as FrequencyExt>::ZERO,
            ingest_rate_burst: usize::MAX,
            max_attr_value: kibibytes(64),
        }
    }
}

impl TenantLimits {
    #[must_use]
    pub fn to_shared_limits(&self) -> Limits {
        Limits {
            ingestion_rate: self.max_ingest_rate,
            ingestion_burst_spans: u64_limit_from_usize(self.ingest_rate_burst),
            max_traces_per_search: Limits::default().max_traces_per_search,
            max_spans_per_trace: u64_limit_from_usize(self.max_spans_per_trace),
            max_attribute: self.max_attr_value,
            max_search_duration: Limits::default().max_search_duration,
        }
    }
}
