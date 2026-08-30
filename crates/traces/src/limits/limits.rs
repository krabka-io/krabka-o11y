use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Limits {
    /// Tempo `ingestion_rate_limit_bytes` analog, counted as spans/sec. Zero is
    /// unlimited.
    #[serde(with = "krabka_units::serde_units::human::frequency")]
    pub ingestion_rate: Frequency,
    /// Tempo `ingestion_burst_size_bytes` analog, counted as spans.
    pub ingestion_burst_spans: u64,
    /// Per-tenant ceiling for `/api/search`'s `limit` query parameter. `0` is
    /// unlimited.
    pub max_traces_per_search: u64,
    /// Tempo `max_bytes_per_trace` analog, counted as spans. `0` is unlimited.
    pub max_spans_per_trace: u64,
    /// Maximum size of any attribute key or string value. Zero is unlimited.
    #[serde(with = "krabka_units::serde_units::human::byte_size")]
    pub max_attribute: ByteSize,
    /// Tempo `max_search_duration`, the `(end-start)` ceiling. Zero is
    /// unlimited.
    #[serde(with = "krabka_units::serde_units::human::time")]
    pub max_search_duration: Time,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            ingestion_rate: per_sec(100_000),
            ingestion_burst_spans: 100_000,
            max_traces_per_search: 1000,
            max_spans_per_trace: 200_000,
            max_attribute: bytes(2048),
            max_search_duration: <Time as TimeExt>::ZERO,
        }
    }
}
