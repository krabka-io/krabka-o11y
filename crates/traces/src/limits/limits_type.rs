use super::{ByteSize, Deserialize, Frequency, Serialize, Time, TimeExt, bytes, hours, per_sec};

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct Limits {
    /// Tempo `ingestion_rate_limit_bytes` analog, counted as spans/sec. Zero is
    /// unlimited.
    #[serde(with = "krabka_units::serde_units::human::frequency")]
    pub ingestion_rate: Frequency,
    /// Tempo `ingestion_burst_size_bytes` analog, counted as spans.
    pub ingestion_burst_spans: u64,
    /// Ceiling on the spans of one push request. Zero is unlimited.
    ///
    /// Tempo has no counterpart. It caps a trace and a rate, and leaves the
    /// request itself to the body-size limit. Krabka decodes the whole request
    /// before it appends, so a request of millions of spans costs the memory
    /// whatever its compressed size was.
    pub max_spans_per_request: u64,
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
    /// How long a tenant's blocks are kept. A zero extent keeps them forever.
    ///
    /// This is Tempo's `block_retention`, with Tempo's default of `336h`. Zero
    /// means "no retention", and it does **not** mean "delete everything": a
    /// tenant whose window is zero keeps every block it ever wrote. Reading it
    /// the other way round would delete the whole of an unconfigured tenant.
    /// The compaction pass reads this window through
    /// [`krabka_blockstore::RetentionWindows`].
    #[serde(with = "krabka_units::serde_units::human::time")]
    pub block_retention: Time,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            ingestion_rate: per_sec(100_000),
            ingestion_burst_spans: 100_000,
            max_spans_per_request: 10_000,
            max_traces_per_search: 1000,
            max_spans_per_trace: 200_000,
            max_attribute: bytes(2048),
            max_search_duration: <Time as TimeExt>::ZERO,
            block_retention: hours(336),
        }
    }
}
