use krabka_traceql::TraceMetricSeries;

use super::{
    TraceMetricsResponse, Value, json, metric_label_json, metric_prom_labels, metric_value_json,
    metrics_operation_name,
};

pub(crate) fn trace_metrics_json(resp: &TraceMetricsResponse, query: &str) -> Value {
    // Tempo `tempopb.QueryRangeResponse` protojson shape, which Grafana's Tempo
    // backend unmarshals: `series[].labels` is an ARRAY of KeyValue, samples use
    // `timestampMs` (milliseconds; int64 rendered as a string to match protojson)
    // and `value`. Krabka's internal point timestamps are nanoseconds.
    let name = metrics_operation_name(query);
    json!({
        "series": resp.series.iter().map(|series| {
            let name = series_name(series, name);
            let mut legend_labels = series.labels.clone();
            if let Some(name) = name {
                legend_labels.push(krabka_traceql::MetricLabel::new("__name__", name));
            }
            json!({
                "labels": metric_labels_json(series, name),
                "promLabels": metric_prom_labels(&legend_labels, &series.label_types),
                "samples": series.points.iter()
                    .map(|point| json!({
                        "timestampMs": (point.timestamp / 1_000_000).to_string(),
                        "value": metric_value_json(point.value),
                    }))
                    .collect::<Vec<_>>(),
                "exemplars": series.exemplars.iter()
                    .map(|exemplar| {
                        json!({
                            "labels": exemplar.labels.iter()
                                .map(|label| metric_label_json(&label.key, &label.value, exemplar.label_types.get(&label.key).copied()))
                                .collect::<Vec<_>>(),
                            "value": metric_value_json(if exemplar.value.is_nan() {
                                series.points.iter()
                                    .find(|point| point.timestamp >= exemplar.timestamp_ns)
                                    .map_or(exemplar.value, |point| point.value)
                            } else {
                                exemplar.value
                            }),
                            "timestampMs": (exemplar.timestamp_ns / 1_000_000).to_string(),
                        })
                    })
                    .collect::<Vec<_>>(),
            })
        }).collect::<Vec<_>>(),
    })
}

pub(crate) fn trace_metrics_instant_json(resp: &TraceMetricsResponse, query: &str) -> Value {
    let name = metrics_operation_name(query);
    super::instant_metrics_response(resp.series.iter().filter_map(|series| {
        series
            .points
            .first()
            .map(|point| super::InstantMetricSample {
                labels: metric_labels_json(series, series_name(series, name)),
                value: point.value,
            })
    }))
}

fn series_name(series: &TraceMetricSeries, name: Option<&'static str>) -> Option<&'static str> {
    name.filter(|_| {
        !series
            .labels
            .iter()
            .any(|label| matches!(label.key.as_str(), "__bucket" | "p"))
    })
}

fn metric_labels_json(series: &TraceMetricSeries, name: Option<&str>) -> Value {
    Value::Array(
        series
            .labels
            .iter()
            .map(|label| {
                metric_label_json(
                    &label.key,
                    &label.value,
                    series.label_types.get(&label.key).copied(),
                )
            })
            .chain(name.map(|name| metric_label_json("__name__", name, None)))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;
    use krabka_traceql::{MetricLabel, MetricPoint, TraceMetricExemplar, TraceMetricSeries};

    use super::*;

    #[test]
    fn instant_serialization_uses_first_sample_and_omits_sampleless_series() {
        let response = TraceMetricsResponse {
            series: vec![
                TraceMetricSeries {
                    label_types: BTreeMap::new(),
                    labels: Vec::new(),
                    points: vec![MetricPoint::new(1, 3.0), MetricPoint::new(2, 99.0)],
                    exemplars: Vec::new(),
                },
                TraceMetricSeries {
                    label_types: BTreeMap::new(),
                    labels: vec![MetricLabel::new("missing", "no-sample")],
                    points: Vec::new(),
                    exemplars: Vec::new(),
                },
            ],
        };
        assert!(
            trace_metrics_instant_json(&response, "{} | count_over_time()")
                == json!({
                    "series":[{"labels":[{"key":"__name__","value":{"stringValue":"count_over_time"}}],"value":3.0}],
                    "metrics":{"completedJobs":1,"totalJobs":1}
                })
        );
    }

    #[test]
    fn placeholder_exemplars_attach_to_right_closed_samples_and_keep_real_values() {
        let response = TraceMetricsResponse {
            series: vec![TraceMetricSeries {
                label_types: BTreeMap::default(),
                labels: Vec::new(),
                points: vec![
                    MetricPoint::new(20_000_000, 3.0),
                    MetricPoint::new(30_000_000, 7.0),
                ],
                exemplars: vec![
                    TraceMetricExemplar {
                        label_types: BTreeMap::default(),
                        labels: vec![MetricLabel::new("trace:id", "123")],
                        value: f64::NAN,
                        timestamp_ns: 20_000_000,
                    },
                    TraceMetricExemplar {
                        label_types: BTreeMap::default(),
                        labels: vec![MetricLabel::new("name", "selected")],
                        value: f64::NAN,
                        timestamp_ns: 21_000_000,
                    },
                    TraceMetricExemplar {
                        label_types: BTreeMap::default(),
                        labels: Vec::new(),
                        value: 0.5,
                        timestamp_ns: 22_000_000,
                    },
                ],
            }],
        };
        let actual = trace_metrics_json(&response, "{} | count_over_time()");
        assert!(
            actual["series"][0]["exemplars"]
                == json!([
                    { "labels": [{"key": "trace:id", "value": {"stringValue": "123"}}], "value": 3.0, "timestampMs": "20" },
                    { "labels": [{"key": "name", "value": {"stringValue": "selected"}}], "value": 7.0, "timestampMs": "21" },
                    { "labels": [], "value": 0.5, "timestampMs": "22" },
                ])
        );
    }
}
