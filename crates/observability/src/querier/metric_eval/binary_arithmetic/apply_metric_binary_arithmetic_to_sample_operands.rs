use super::{
    MetricScalarArithmeticOp, SampleOperands, Value, format_metric_value, json,
    metric_binary_operand_values, metric_scalar_arithmetic_value,
};

pub(crate) fn apply_metric_binary_arithmetic_to_sample_operands(
    output_sample: &mut Value,
    left_sample: &Value,
    right_sample: &Value,
    op: MetricScalarArithmeticOp,
) -> bool {
    let Some(output_values) = output_sample.as_array_mut() else {
        return false;
    };
    let Some((left_value, right_value)) = metric_binary_operand_values(SampleOperands {
        left: left_sample,
        right: right_sample,
    }) else {
        return false;
    };
    let Some(result) = metric_scalar_arithmetic_value(left_value, op, right_value, false) else {
        return false;
    };
    if let Some(value) = output_values.get_mut(1) {
        *value = json!(format_metric_value(result));
    }
    true
}
