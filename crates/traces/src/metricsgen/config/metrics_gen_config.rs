use super::{DEFAULT_LATENCY_BUCKETS_NS, Deserialize, Serialize, Time, secs};

/// Metrics-generator runtime configuration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsGenConfig {
    #[serde(
        rename = "collection_interval_secs",
        with = "krabka_units::serde_units::numeric::secs_i64"
    )]
    pub collection_interval: Time,
    pub histogram_buckets_ns: Vec<f64>,
    pub max_exemplars_per_series: usize,
    #[serde(
        rename = "edge_ttl_secs",
        with = "krabka_units::serde_units::numeric::secs_i64"
    )]
    pub edge_ttl: Time,
    pub edge_store_max_items: usize,
    /// Ceiling on the span-metrics dimension keys one tenant may hold. Zero is
    /// unlimited.
    ///
    /// This is Tempo's `metrics_generator.max_active_series` key, with Tempo's
    /// semantics: a dimension key already in the registry is always recorded,
    /// and only a new key is refused once the registry is full. One entry here
    /// carries the three RED series and the `traces_target_info` gauge that the
    /// key produces, so the count is of keys, not of emitted series.
    ///
    /// The default diverges from Tempo, which ships `0` and so leaves the
    /// registry unbounded. A span name is the classic unbounded dimension of a
    /// trace, and one tenant that puts a SQL statement or a job id there grows
    /// this map, and the metrics store behind `remote_write`, without end.
    /// The number matches `edge_store_max_items`, so both processors of the
    /// generator hold the same cardinality budget.
    pub max_active_series: usize,
    /// Ceiling on the tenants one generator holds state for. Zero is unlimited.
    ///
    /// Tempo has no counterpart: it shards tenants over a ring. Krabka reads
    /// every tenant from one WAL topic, so the tenant map is a third unbounded
    /// map and needs its own cap.
    pub max_tenants: usize,
    pub enable_target_info: bool,
    pub enable_status_message: bool,
    pub enable_messaging_system_latency: bool,
    pub remote_write_url: String,
}

impl Default for MetricsGenConfig {
    fn default() -> Self {
        Self {
            collection_interval: secs(15),
            histogram_buckets_ns: DEFAULT_LATENCY_BUCKETS_NS.to_vec(),
            max_exemplars_per_series: 0,
            edge_ttl: secs(10),
            edge_store_max_items: 10_000,
            max_active_series: 10_000,
            max_tenants: 10_000,
            enable_target_info: false,
            enable_status_message: false,
            enable_messaging_system_latency: false,
            remote_write_url: "http://localhost:9009/api/v1/push".to_string(),
        }
    }
}
