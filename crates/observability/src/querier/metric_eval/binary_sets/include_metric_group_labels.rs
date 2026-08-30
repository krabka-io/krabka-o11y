use super::{Value, json};

pub(crate) fn include_metric_group_labels(
    output_series: &mut Value,
    source_series: &Value,
    labels: &[String],
) {
    if labels.is_empty() {
        return;
    }
    let Some(source_metric) = source_series.get("metric").and_then(Value::as_object) else {
        return;
    };
    let Some(output_metric) = output_series
        .get_mut("metric")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    for label in labels {
        output_metric.remove(label);
        if let Some(value) = source_metric.get(label).and_then(Value::as_str) {
            output_metric.insert(label.clone(), json!(value));
        }
    }
}
