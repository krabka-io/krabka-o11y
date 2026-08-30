use super::*;

pub(crate) fn apply_label_join_to_loki_result(value: &mut Value, label_join: &MetricLabelJoin) {
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };

    for series in results {
        let Some(metric) = series.get_mut("metric").and_then(Value::as_object_mut) else {
            continue;
        };
        let joined = label_join
            .source_labels
            .iter()
            .map(|label| metric.get(label).and_then(Value::as_str).unwrap_or(""))
            .collect::<Vec<_>>()
            .join(&label_join.separator);
        metric.insert(label_join.destination_label.clone(), json!(joined));
    }
}
