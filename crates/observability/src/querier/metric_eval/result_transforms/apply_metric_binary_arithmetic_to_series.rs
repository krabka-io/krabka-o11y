use super::{
    MetricScalarArithmeticOp, Value, apply_metric_binary_arithmetic_to_sample,
    matching_metric_binary_sample,
};

pub(crate) fn apply_metric_binary_arithmetic_to_series(
    left_series: &mut Value,
    right_series: &Value,
    op: MetricScalarArithmeticOp,
) -> bool {
    if let Some(left_values) = left_series.get_mut("values").and_then(Value::as_array_mut) {
        let Some(right_values) = right_series.get("values").and_then(Value::as_array) else {
            return false;
        };
        let mut index = 0;
        while index < left_values.len() {
            let Some(right_sample) =
                matching_metric_binary_sample(&left_values[index], right_values)
            else {
                left_values.remove(index);
                continue;
            };
            if apply_metric_binary_arithmetic_to_sample(&mut left_values[index], right_sample, op) {
                index += 1;
            } else {
                left_values.remove(index);
            }
        }
        return !left_values.is_empty();
    }

    let Some(left_sample) = left_series.get_mut("value") else {
        return false;
    };
    let Some(right_sample) = right_series.get("value") else {
        return false;
    };
    apply_metric_binary_arithmetic_to_sample(left_sample, right_sample, op)
}
