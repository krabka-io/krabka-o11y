use super::{
    ByteSize, Deserialize, Frequency, Serialize, Time, TimeExt, kibibytes, minutes,
    non_negative_time, per_sec, serde_units,
};

/// Mimir-style per-tenant limits used by metrics ingest and query paths.
///
/// Every ingest gate reads this one type, and the push path resolves it once
/// per request. A limit that only some gates honour would give one request two
/// verdicts on the same label.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Limits {
    /// Accepted sample rate. A zero rate turns the ingestion rate limit off.
    #[serde(with = "serde_units::human::frequency")]
    pub ingestion_rate: Frequency,
    /// Samples the token bucket may hand out in one burst.
    pub ingestion_burst_size: u64,
    /// Active series per tenant. `0` turns the cap off.
    ///
    /// A series counts as active until the tenant stops writing to it for
    /// [`Limits::active_series_idle_timeout`].
    pub max_global_series_per_user: u64,
    /// Series one push request may carry.
    pub max_series_per_request: u64,
    /// Samples, native histograms and exemplars one series may carry in one
    /// push request, counted together.
    pub max_samples_per_series: u64,
    #[serde(with = "serde_units::human::byte_size")]
    pub max_label_name_length: ByteSize,
    #[serde(with = "serde_units::human::byte_size")]
    pub max_label_value_length: ByteSize,
    /// How long a series stays active after the last write to it. A zero
    /// extent keeps every series that was ever written.
    ///
    /// This is Mimir's `-ingester.active-series-metrics-idle-timeout`, with
    /// Mimir's default.
    #[serde(with = "non_negative_time")]
    pub active_series_idle_timeout: Time,
    /// How long an OTLP delta stream keeps its accumulated cumulative value
    /// after the last point. A zero extent keeps every stream.
    ///
    /// This is the `max_stale` of the OpenTelemetry Collector
    /// `deltatocumulative` processor, with that processor's default.
    #[serde(with = "non_negative_time")]
    pub otlp_delta_max_stale: Time,
    /// OTLP delta streams one tenant may accumulate. `0` turns the cap off.
    ///
    /// This is the `max_streams` of the OpenTelemetry Collector
    /// `deltatocumulative` processor. That processor defaults the cap to the
    /// largest machine integer, which is the unbounded growth this limit
    /// exists to stop, so Krabka gives it a finite default instead.
    pub otlp_delta_max_streams: u64,
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
    /// How long a tenant's compacted blocks are kept. A zero extent keeps them
    /// forever.
    ///
    /// This is Mimir's `compactor_blocks_retention_period`, with Mimir's
    /// default of zero. Zero means "no retention", and it does **not** mean
    /// "delete everything": a tenant that configures nothing keeps every block
    /// it ever wrote. The retention sweep reads this window through
    /// [`krabka_blockstore::RetentionWindows`].
    #[serde(with = "non_negative_time")]
    pub compactor_blocks_retention_period: Time,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            // The rate, burst and label-length defaults are Mimir's. They are
            // stricter than a request-shape cap has to be, and that is the
            // point: a tenant with no override gets the same budget Mimir
            // would give it.
            ingestion_rate: per_sec(10_000),
            ingestion_burst_size: 200_000,
            max_global_series_per_user: 150_000,
            max_series_per_request: 100_000,
            max_samples_per_series: 10_000,
            max_label_name_length: kibibytes(1),
            max_label_value_length: kibibytes(2),
            active_series_idle_timeout: minutes(20),
            otlp_delta_max_stale: minutes(5),
            otlp_delta_max_streams: 100_000,
            max_samples_per_query: 50_000_000,
            max_fetched_series_per_query: 100_000,
            max_query_lookback: Time::ZERO,
            max_query_length: Time::ZERO,
            out_of_order_time_window: Time::ZERO,
            compactor_blocks_retention_period: Time::ZERO,
        }
    }
}
