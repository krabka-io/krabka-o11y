//! Typed merge of `TraceQL`-metrics series across shards.
//!
//! The merge unions series by their label set, sums samples at equal
//! timestamps, concatenates exemplars, and applies exemplar limiting.
//!
//! The serde structs are shaped to the querier's `trace_metrics_json` body,
//! which is Tempo's protojson `QueryRangeResponse`. `labels` is a `KeyValue`
//! array. Each entry in `samples` carries a `timestampMs`, an int64 count of
//! milliseconds rendered as a string, plus a `value`. The body also carries
//! `promLabels` and exemplars.
//!
//! This is the same shape Grafana's Tempo backend unmarshals. The frontend
//! therefore both decodes per-shard querier responses and re-serializes the
//! merged result correctly.

use serde::{Deserialize, Serialize};

#[cfg(test)]
mod tests {

    use super::*;

    fn labels(svc: &str) -> Vec<KeyValue> {
        vec![KeyValue {
            key: "svc".to_string(),
            value: serde_json::json!({ "stringValue": svc }),
        }]
    }

    fn sample(ts_ms: &str, value: f64) -> MetricSample {
        MetricSample {
            timestamp_ms: ts_ms.to_string(),
            value,
        }
    }

    #[test]
    fn merges_samples_with_same_timestamp() {
        let a = MetricSeries {
            labels: labels("api"),
            prom_labels: "{svc=\"api\"}".into(),
            samples: vec![sample("1000", 2.0), sample("2000", 4.0)],
            exemplars: vec![],
        };
        let b = MetricSeries {
            labels: labels("api"),
            prom_labels: "{svc=\"api\"}".into(),
            samples: vec![sample("1000", 3.0), sample("3000", 5.0)],
            exemplars: vec![],
        };
        let mut merged = Vec::new();
        merge_metric_series(&mut merged, a);
        merge_metric_series(&mut merged, b);
        assert2::assert!(merged.len() == 1);
        assert2::assert!(
            merged[0].samples.as_slice()
                == &[
                    sample("1000", 5.0),
                    sample("2000", 4.0),
                    sample("3000", 5.0)
                ][..]
        );
    }

    #[test]
    fn distinct_label_sets_stay_separate() {
        let a = MetricSeries {
            labels: labels("api"),
            prom_labels: String::new(),
            samples: vec![],
            exemplars: vec![],
        };
        let b = MetricSeries {
            labels: labels("db"),
            prom_labels: String::new(),
            samples: vec![],
            exemplars: vec![],
        };
        let mut merged = Vec::new();
        merge_metric_series(&mut merged, a);
        merge_metric_series(&mut merged, b);
        assert2::assert!(merged.len() == 2);
    }

    #[test]
    fn exemplar_limit_truncates() {
        let mut series = vec![MetricSeries {
            labels: labels("api"),
            prom_labels: String::new(),
            samples: vec![],
            exemplars: vec![
                Exemplar {
                    labels: vec![],
                    value: 1.0,
                    timestamp_ms: "1".into(),
                },
                Exemplar {
                    labels: vec![],
                    value: 2.0,
                    timestamp_ms: "2".into(),
                },
            ],
        }];
        limit_exemplars(&mut series, Some(1));
        assert2::assert!(series[0].exemplars.len() == 1);
    }

    #[test]
    fn merge_metrics_end_to_end() {
        let p0 = MetricsResponseJson {
            series: vec![MetricSeries {
                labels: labels("api"),
                prom_labels: "{svc=\"api\"}".into(),
                samples: vec![sample("1", 1.0)],
                exemplars: vec![Exemplar {
                    labels: vec![],
                    value: 1.0,
                    timestamp_ms: "1".into(),
                }],
            }],
        };
        let p1 = MetricsResponseJson {
            series: vec![MetricSeries {
                labels: labels("api"),
                prom_labels: "{svc=\"api\"}".into(),
                samples: vec![sample("1", 2.0)],
                exemplars: vec![Exemplar {
                    labels: vec![],
                    value: 2.0,
                    timestamp_ms: "2".into(),
                }],
            }],
        };
        let merged = merge_metrics(vec![p0, p1], Some(1));
        assert2::assert!(
            merged
                == MetricsResponseJson {
                    series: vec![MetricSeries {
                        labels: labels("api"),
                        prom_labels: "{svc=\"api\"}".to_string(),
                        samples: vec![sample("1", 3.0)],
                        exemplars: vec![Exemplar {
                            labels: vec![],
                            value: 1.0,
                            timestamp_ms: "1".to_string(),
                        }],
                    }],
                }
        );
    }

    #[test]
    fn round_trips_querier_metrics_body() {
        // Exactly the shape the querier's `trace_metrics_json` emits.
        let body = serde_json::json!({
            "series": [{
                "labels": [{"key": "svc", "value": {"stringValue": "api"}}],
                "promLabels": "{svc=\"api\"}",
                "samples": [{"timestampMs": "1000", "value": 2.0}],
                "exemplars": [{
                    "labels": [{"key": "trace_id", "value": {"stringValue": "0a"}}],
                    "value": 1.5,
                    "timestampMs": "1000"
                }]
            }]
        });
        let resp: MetricsResponseJson = serde_json::from_value(body.clone()).unwrap();
        assert2::assert!(
            resp == MetricsResponseJson {
                series: vec![MetricSeries {
                    labels: vec![KeyValue {
                        key: "svc".to_string(),
                        value: serde_json::json!({ "stringValue": "api" }),
                    }],
                    prom_labels: "{svc=\"api\"}".to_string(),
                    samples: vec![sample("1000", 2.0)],
                    exemplars: vec![Exemplar {
                        labels: vec![KeyValue {
                            key: "trace_id".to_string(),
                            value: serde_json::json!({ "stringValue": "0a" }),
                        }],
                        value: 1.5,
                        timestamp_ms: "1000".to_string(),
                    }],
                }],
            }
        );
        // Re-serializes to the same Tempo shape (round-trip stable).
        assert2::assert!(serde_json::to_value(&resp).unwrap() == body);
    }
}

// === split-modules: generated submodules ===
mod exemplar;
mod key_value;
mod limit_exemplars;
mod merge_metric_series;
mod merge_metrics;
mod merge_samples;
mod metric_sample;
mod metric_series;
mod metrics_response_json;

pub use exemplar::Exemplar;
pub use key_value::KeyValue;
pub use limit_exemplars::limit_exemplars;
pub use merge_metric_series::merge_metric_series;
pub use merge_metrics::merge_metrics;
use merge_samples::merge_samples;
pub use metric_sample::MetricSample;
pub use metric_series::MetricSeries;
pub use metrics_response_json::MetricsResponseJson;
