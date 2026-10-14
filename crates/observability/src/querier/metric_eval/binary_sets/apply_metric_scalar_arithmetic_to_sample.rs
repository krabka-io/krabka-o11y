use super::{
    MetricValue, ScalarArithmetic, Value, format_metric_value, json,
    metric_scalar_arithmetic_value, scalar_operands_for_sample,
};

pub(crate) fn apply_metric_scalar_arithmetic_to_sample(
    sample: &mut Value,
    arithmetic: ScalarArithmetic,
    scalar: MetricValue,
) -> bool {
    let Some((slot, operands)) = scalar_operands_for_sample(sample, scalar, arithmetic.scalar_side)
    else {
        return false;
    };
    let Some(result) = metric_scalar_arithmetic_value(operands, arithmetic.op) else {
        return false;
    };
    *slot = json!(format_metric_value(result));
    true
}
