use super::*;

pub(crate) fn apply_metric_scalar_arithmetic_to_sample(
    sample: &mut Value,
    op: MetricScalarArithmeticOp,
    scalar: MetricValue,
    scalar_on_left: bool,
) -> bool {
    let Some(values) = sample.as_array_mut() else {
        return false;
    };
    let Some(sample_value) = values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let Some(result) = metric_scalar_arithmetic_value(sample_value, op, scalar, scalar_on_left)
    else {
        return false;
    };
    if let Some(value) = values.get_mut(1) {
        *value = json!(format_metric_value(result));
    }
    true
}
