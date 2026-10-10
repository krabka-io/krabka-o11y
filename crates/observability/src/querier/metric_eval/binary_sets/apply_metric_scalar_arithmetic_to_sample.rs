use super::{
    MetricValue, ScalarArithmetic, ScalarOperands, Value, format_metric_value, json,
    metric_scalar_arithmetic_value, parse_metric_sample_value,
};

pub(crate) fn apply_metric_scalar_arithmetic_to_sample(
    sample: &mut Value,
    arithmetic: ScalarArithmetic,
    scalar: MetricValue,
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
    let operands = ScalarOperands {
        sample: sample_value,
        scalar,
        scalar_side: arithmetic.scalar_side,
    };
    let Some(result) = metric_scalar_arithmetic_value(operands, arithmetic.op) else {
        return false;
    };
    if let Some(value) = values.get_mut(1) {
        *value = json!(format_metric_value(result));
    }
    true
}
