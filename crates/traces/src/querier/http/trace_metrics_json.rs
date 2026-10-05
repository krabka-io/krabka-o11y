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
                    .map(|(key, value)| metric_label_json(key, value))
                    .chain(name.map(|name| metric_label_json("__name__", name)))
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
                                .map(|(key, value)| metric_label_json(key, value))
                                .collect::<Vec<_>>(),
                            "value": metric_value_json(exemplar.value),
                            "timestampMs": (exemplar.timestamp_ns / 1_000_000).to_string(),
                        })
                    })
                    .collect::<Vec<_>>(),
            })
        }).collect::<Vec<_>>(),
    })
}
