use serde::Deserialize;
use serde_json::{Value, json};

use crate::frontend::{
    backend::MetricsPartial,
    metrics_merge::{KeyValue, MetricSample, MetricSeries, MetricsResponseJson},
    wire::Metrics,
};

#[derive(Deserialize)]
pub(super) struct InstantMetricsResponseJson {
    #[serde(default)]
    series: Vec<InstantSeries>,
    #[serde(default)]
    metrics: Metrics,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InstantSeries {
    #[serde(default)]
    labels: Vec<KeyValue>,
    #[serde(default = "zero_value")]
    value: Value,
}

fn zero_value() -> Value {
    json!(0.0)
}

impl InstantMetricsResponseJson {
    pub(super) fn into_partial(self, point_ns: i64) -> Result<MetricsPartial, String> {
        let series = self
            .series
            .into_iter()
            .map(|series| {
                let value = series
                    .value
                    .as_f64()
                    .or_else(|| match series.value.as_str() {
                        Some("NaN") => Some(f64::NAN),
                        Some("Infinity") => Some(f64::INFINITY),
                        Some("-Infinity") => Some(f64::NEG_INFINITY),
                        _ => None,
                    })
                    .ok_or_else(|| "invalid instant metric value".to_string())?;
                Ok(MetricSeries {
                    labels: series.labels,
                    prom_labels: String::new(),
                    samples: vec![MetricSample {
                        timestamp_ms: (point_ns / 1_000_000).to_string(),
                        value,
                    }],
                    exemplars: Vec::new(),
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(MetricsPartial {
            response: MetricsResponseJson { series },
            metrics: self.metrics,
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn scalar_replies_keep_typed_labels_accounting_and_internal_point() {
        let body = json!({"series": [{
            "labels": [{"key": "kind", "value": {"intValue": "7"}}], "value": 3.0
        }], "metrics": {"totalJobs": 4, "completedJobs": 3}});
        let response: InstantMetricsResponseJson = serde_json::from_value(body).unwrap();
        let partial = response.into_partial(1_700_000_000_123_456_789).unwrap();
        assert!(
            partial.response
                == MetricsResponseJson {
                    series: vec![MetricSeries {
                        labels: vec![KeyValue {
                            key: "kind".into(),
                            value: json!({"intValue":"7"})
                        }],
                        prom_labels: String::new(),
                        samples: vec![MetricSample {
                            timestamp_ms: "1700000000123".into(),
                            value: 3.0
                        }],
                        exemplars: Vec::new(),
                    }]
                }
        );
        assert!(
            partial.metrics
                == Metrics {
                    total_jobs: 4,
                    completed_jobs: 3,
                    ..Metrics::default()
                }
        );
        for (value, expected) in [
            (json!("NaN"), f64::NAN),
            (json!("Infinity"), f64::INFINITY),
            (json!("-Infinity"), f64::NEG_INFINITY),
        ] {
            let reply: InstantMetricsResponseJson =
                serde_json::from_value(json!({"series":[{"value":value}]})).unwrap();
            assert!(
                reply.into_partial(0).unwrap().response.series[0].samples[0]
                    .value
                    .to_bits()
                    == expected.to_bits()
            );
        }
        let omitted: InstantMetricsResponseJson =
            serde_json::from_value(json!({"series":[{}]})).unwrap();
        assert!(
            omitted.into_partial(0).unwrap().response.series[0].samples[0]
                .value
                .to_bits()
                == 0.0_f64.to_bits()
        );
    }

    #[test]
    fn range_fields_and_invalid_scalars_cannot_become_silent_zeroes() {
        for field in ["samples", "exemplars", "promLabels"] {
            let mut series = json!({"value":3});
            series[field] = json!([]);
            assert!(
                serde_json::from_value::<InstantMetricsResponseJson>(json!({"series":[series]}))
                    .is_err()
            );
        }
        for value in [
            json!(null),
            json!(true),
            json!("3"),
            json!("+Inf"),
            json!({}),
        ] {
            let reply: InstantMetricsResponseJson =
                serde_json::from_value(json!({"series":[{"value":value}]})).unwrap();
            assert!(reply.into_partial(0).is_err());
        }
    }
}
