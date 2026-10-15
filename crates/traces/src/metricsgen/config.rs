//! Metrics-generator configuration.

use std::collections::HashMap;

use krabka_units::{Time, secs};
use serde::{Deserialize, Serialize};


/// Tempo-default latency histogram bucket edges, in nanoseconds.
pub const DEFAULT_LATENCY_BUCKETS_NS: &[f64] = &[
    2_000_000.0,
    4_000_000.0,
    8_000_000.0,
    16_000_000.0,
    32_000_000.0,
    64_000_000.0,
    128_000_000.0,
    256_000_000.0,
    512_000_000.0,
    1_024_000_000.0,
    2_048_000_000.0,
    4_096_000_000.0,
    8_192_000_000.0,
    16_384_000_000.0,
];

#[cfg(test)]
mod tests {
    use super::*;

    /// Two fields diverge on purpose, and both say why at their definition.
    /// `max_active_series` is `0` in Tempo, which leaves the span-metrics
    /// registry unbounded, and `max_tenants` has no Tempo counterpart at all.
    /// Every other default is Tempo's.
    #[test]
    fn defaults_match_tempo() {
        // Compared as the serialized document, so the check covers the
        // Tempo-spelled keys each default is written under too.
        let defaults = serde_json::to_value(MetricsGenConfig::default()).unwrap();
        assert2::assert!(
            defaults
                == serde_json::json!({
                    "collection_interval_secs": 15,
                    "histogram_buckets_ns": DEFAULT_LATENCY_BUCKETS_NS,
                    "max_exemplars_per_series": 0,
                    "edge_ttl_secs": 10,
                    "edge_store_max_items": 10_000,
                    "max_active_series": 10_000,
                    "max_tenants": 10_000,
                    "enable_target_info": false,
                    "enable_status_message": false,
                    "enable_messaging_system_latency": false,
                    "remote_write_url": "http://localhost:9009/api/v1/push",
                    "processor": ProcessorConfig::default(),
                    "overrides": {},
                })
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
                max_exemplars_per_series: 5,
                ..MetricsGenConfig::default()
            }
        );
    }

    #[test]
    fn processor_overrides_share_the_trace_limits_document() {
        let mut config = MetricsGenConfig::default();
        config
            .apply_runtime_overrides(
                r"
overrides:
  tenant-a:
    max_spans_per_trace: 10
    metrics_generator:
      processor:
        span_metrics:
          dimensions: [http.method]
",
            )
            .unwrap();

        assert2::assert!(
            config
                .for_tenant("tenant-a")
                .processor
                .span_metrics
                .dimensions
                == ["http.method"]
        );
        assert2::assert!(
            config
                .for_tenant("tenant-b")
                .processor
                .span_metrics
                .dimensions
                .is_empty()
        );
    }
}

mod metrics_gen_config;
mod processor_config;

pub use metrics_gen_config::MetricsGenConfig;
pub use processor_config::{
    AttributeMatch, DimensionMapping, FilterPolicy, HostInfoConfig, MatchType, ProcessorConfig,
    ServiceGraphsConfig, SpanMetricsConfig,
};
