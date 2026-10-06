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
            json!({
                "labels": series.labels.iter()
                    .map(|(key, value)| metric_label_json(key, value, series.label_types.get(key).copied()))
                    .chain(name.filter(|_| !series.labels.iter().any(|(key, _)| matches!(key.as_str(), "__bucket" | "p"))).map(|name| metric_label_json("__name__", name, None)))
                    .collect::<Vec<_>>(),
                "promLabels": metric_prom_labels(&series.labels),
                "samples": series.points.iter()
                    .map(|(ts_ns, value)| json!({
                        "timestampMs": (ts_ns / 1_000_000).to_string(),
                        "value": metric_value_json(*value),
                    }))
                    .collect::<Vec<_>>(),
                "exemplars": series.exemplars.iter()
                    .map(|exemplar| {
                        json!({
                            "labels": exemplar.labels.iter()
                                .map(|(key, value)| metric_label_json(key, value, exemplar.label_types.get(key).copied()))
                                .collect::<Vec<_>>(),
                            "value": metric_value_json(if exemplar.value.is_nan() {
                                series.points.iter()
                                    .find(|(timestamp, _)| *timestamp >= exemplar.timestamp_ns)
                                    .map_or(exemplar.value, |(_, value)| *value)
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::assert;
    use krabka_traceql::{TraceMetricExemplar, TraceMetricSeries};

    use super::*;

    #[test]
    fn placeholder_exemplars_attach_to_right_closed_samples_and_keep_real_values() {
        let response = TraceMetricsResponse {
            series: vec![TraceMetricSeries {
                label_types: BTreeMap::default(),
                labels: Vec::new(),
                points: vec![(20_000_000, 3.0), (30_000_000, 7.0)],
                exemplars: vec![
                    TraceMetricExemplar {
                        label_types: BTreeMap::default(),
                        labels: vec![("trace:id".into(), "123".into())],
                        value: f64::NAN,
                        timestamp_ns: 20_000_000,
                    },
                    TraceMetricExemplar {
                        label_types: BTreeMap::default(),
                        labels: vec![("name".into(), "selected".into())],
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
