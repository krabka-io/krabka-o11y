use super::{MetricLabelJoin, Value, json};

pub(crate) fn apply_label_join_to_loki_result(value: &mut Value, label_join: &MetricLabelJoin) {
    apply_label_join_fields(
        value,
        &label_join.destination_label,
        &label_join.separator,
        &label_join.source_labels,
    );
}

pub(crate) fn apply_label_join_fields(
    value: &mut Value,
    destination_label: &str,
    separator: &str,
    source_labels: &[String],
) {
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
        let joined = source_labels
            .iter()
            .map(|label| metric.get(label).and_then(Value::as_str).unwrap_or(""))
            .collect::<Vec<_>>()
            .join(separator);
        metric.insert(destination_label.to_string(), json!(joined));
    }
}
