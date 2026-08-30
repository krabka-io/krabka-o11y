use super::{ByteRateExt, ByteSize, ByteSizeExt, Deserialize, Frequency, FrequencyExt, RatioExt, Serialize, Time, TimeExt, kibibytes, non_negative_time, per_sec, serde_units};

/// Mimir-style per-tenant limits used by metrics ingest and query paths.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Limits {
    /// Accepted sample rate. A zero rate turns the ingestion rate limit off.
    #[serde(with = "serde_units::human::frequency")]
    pub ingestion_rate: Frequency,
    /// Samples the token bucket may hand out in one burst.
    pub ingestion_burst_size: u64,
    /// Active series per tenant. `0` turns the cap off.
    pub max_global_series_per_user: u64,
    #[serde(with = "serde_units::human::byte_size")]
    pub max_label_name_length: ByteSize,
    #[serde(with = "serde_units::human::byte_size")]
    pub max_label_value_length: ByteSize,
    pub max_samples_per_query: u64,
    pub max_fetched_series_per_query: u64,
    /// How far back a query may reach. A zero extent turns the cap off.
    #[serde(with = "non_negative_time")]
    pub max_query_lookback: Time,
    /// The widest span a range query may cover. A zero extent turns the cap
    /// off.
    #[serde(with = "non_negative_time")]
    pub max_query_length: Time,
    /// Accepted out-of-order ingest window. A negative extent turns the cap
    /// off.
    #[serde(with = "serde_units::human::time")]
    pub out_of_order_time_window: Time,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            ingestion_rate: per_sec(10_000),
            ingestion_burst_size: 200_000,
            max_global_series_per_user: 150_000,
            max_label_name_length: kibibytes(1),
            max_label_value_length: kibibytes(2),
            max_samples_per_query: 50_000_000,
            max_fetched_series_per_query: 100_000,
            max_query_lookback: Time::ZERO,
            max_query_length: Time::ZERO,
            out_of_order_time_window: Time::ZERO,
        }
    }
}
