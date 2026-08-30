//! Metrics-generator configuration.

use krabka_units::{Time, secs};
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_tempo() {
        let c = MetricsGenConfig::default();
        assert2::assert!(
            c == MetricsGenConfig {
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
        );
    }

    #[test]
    fn parses_partial_yaml_falling_back_to_defaults() {
        let c: MetricsGenConfig =
            serde_yaml::from_str("collection_interval_secs: 30\nmax_exemplars_per_series: 5\n")
                .unwrap();
        assert2::assert!(
            c == MetricsGenConfig {
                collection_interval: secs(30),
                histogram_buckets_ns: DEFAULT_LATENCY_BUCKETS_NS.to_vec(),
                max_exemplars_per_series: 5,
                edge_ttl: secs(10),
                edge_store_max_items: 10_000,
                enable_target_info: false,
                enable_status_message: false,
                enable_messaging_system_latency: false,
                remote_write_url: "http://localhost:9009/api/v1/push".to_string(),
            }
        );
    }
}

mod default_latency_buckets_ns;
mod metrics_gen_config;

pub use default_latency_buckets_ns::DEFAULT_LATENCY_BUCKETS_NS;
pub use metrics_gen_config::MetricsGenConfig;
