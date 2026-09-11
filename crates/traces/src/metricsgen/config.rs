//! Metrics-generator configuration.

use krabka_units::{Time, secs};
use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests {
    use super::*;

    /// Two fields diverge on purpose, and both say why at their definition.
    /// `max_active_series` is `0` in Tempo, which leaves the span-metrics
    /// registry unbounded, and `max_tenants` has no Tempo counterpart at all.
    /// Every other default is Tempo's.
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
                max_active_series: 10_000,
                max_tenants: 10_000,
                enable_target_info: false,
                enable_status_message: false,
                enable_messaging_system_latency: false,
                remote_write_url: "http://localhost:9009/api/v1/push".to_string(),
            }
        );
    }

    /// The cardinality caps are read under the keys Tempo spells them with, so
    /// an operator's file reaches them. A rename of either field would leave
    /// `serde` to fill the default in silence, and the map would stay as wide
    /// as the process default however small a number the file named.
    #[test]
    fn cardinality_caps_come_from_the_tempo_spelled_keys() {
        let c: MetricsGenConfig =
            serde_yaml::from_str("max_active_series: 3\nmax_tenants: 7\n").unwrap();

        assert2::assert!(
            c == MetricsGenConfig {
                max_active_series: 3,
                max_tenants: 7,
                ..MetricsGenConfig::default()
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
                max_active_series: 10_000,
                max_tenants: 10_000,
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
