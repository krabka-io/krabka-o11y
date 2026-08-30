use super::{MetricVectorMatching, Value};

pub(crate) fn retain_metric_binary_on_labels(
    value: &mut Value,
    matching: Option<&MetricVectorMatching>,
) {
    let Some(MetricVectorMatching::On {
        labels,
        group: None,
    }) = matching
    else {
        return;
    };
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
        metric.retain(|label, _| labels.contains(label));
    }
}
