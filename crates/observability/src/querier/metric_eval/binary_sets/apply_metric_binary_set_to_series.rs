use super::{
    MetricBinarySetOp, Value, matching_metric_binary_sample, metric_binary_set_keeps_sample,
    metric_samples_share_timestamp,
};

pub(crate) fn apply_metric_binary_set_to_series(
    left_series: &mut Value,
    right_series: &Value,
    op: MetricBinarySetOp,
) -> bool {
    if let Some(left_values) = left_series.get_mut("values").and_then(Value::as_array_mut) {
        let right_values = right_series.get("values").and_then(Value::as_array);
        let mut index = 0;
        while index < left_values.len() {
            let matched = right_values
                .and_then(|right_values| {
                    matching_metric_binary_sample(&left_values[index], right_values)
                })
                .is_some();
            if metric_binary_set_keeps_sample(op, matched) {
                index += 1;
            } else {
                left_values.remove(index);
            }
        }
        return !left_values.is_empty();
    }

    let Some(left_sample) = left_series.get("value") else {
        return false;
    };
    let matched = right_series
        .get("value")
        .is_some_and(|right_sample| metric_samples_share_timestamp(left_sample, right_sample));
    metric_binary_set_keeps_sample(op, matched)
}
