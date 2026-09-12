use super::{Deserialize, Time};

// The Tempo-shaped runtime-overrides keys, in the units an operator writes them
// (spans/sec, bytes, seconds). This is intentionally partial configuration, not
// old-schema compatibility: each tenant entry overrides only the limit fields it
// names, and `merge_limits` lifts them into the dimensioned `Limits`.
#[derive(Default, Deserialize)]
#[serde(default)]
pub(crate) struct PartialLimits {
    pub(crate) ingestion_rate_spans_per_sec: Option<f64>,
    pub(crate) ingestion_burst_spans: Option<u64>,
    pub(crate) max_spans_per_request: Option<u64>,
    pub(crate) max_traces_per_search: Option<u64>,
    pub(crate) max_spans_per_trace: Option<u64>,
    pub(crate) max_attribute_bytes: Option<u64>,
    pub(crate) max_search_duration_secs: Option<u64>,
    // Tempo writes this one as a duration rather than a count, and it is the
    // only knob here an operator copies straight out of a Tempo overrides
    // file, so it keeps Tempo's spelling and Tempo's value form: `336h`.
    #[serde(with = "krabka_units::serde_units::human::option_time")]
    pub(crate) block_retention: Option<Time>,
}
