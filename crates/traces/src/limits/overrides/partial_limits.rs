use super::Deserialize;

// The Tempo-shaped runtime-overrides keys, in the units an operator writes them
// (spans/sec, bytes, seconds). This is intentionally partial configuration, not
// old-schema compatibility: each tenant entry overrides only the limit fields it
// names, and `merge_limits` lifts them into the dimensioned `Limits`.
#[derive(Default, Deserialize)]
#[serde(default)]
pub(crate) struct PartialLimits {
    pub(crate) ingestion_rate_spans_per_sec: Option<f64>,
    pub(crate) ingestion_burst_spans: Option<u64>,
    pub(crate) max_traces_per_search: Option<u64>,
    pub(crate) max_spans_per_trace: Option<u64>,
    pub(crate) max_attribute_bytes: Option<u64>,
    pub(crate) max_search_duration_secs: Option<u64>,
}
