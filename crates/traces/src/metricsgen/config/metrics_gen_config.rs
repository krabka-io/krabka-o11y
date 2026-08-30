use super::*;

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
            enable_target_info: false,
            enable_status_message: false,
            enable_messaging_system_latency: false,
            remote_write_url: "http://localhost:9009/api/v1/push".to_string(),
        }
    }
}
