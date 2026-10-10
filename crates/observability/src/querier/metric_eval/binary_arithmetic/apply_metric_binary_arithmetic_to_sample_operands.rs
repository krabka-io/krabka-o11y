use super::{
    MetricScalarArithmeticOp, SampleOperands, ScalarOperands, ScalarSide, Value,
    format_metric_value, json, metric_binary_operand_values, metric_scalar_arithmetic_value,
};

pub(crate) fn apply_metric_binary_arithmetic_to_sample_operands(
    output_sample: &mut Value,
    operands: SampleOperands<'_>,
    op: MetricScalarArithmeticOp,
) -> bool {
    let Some(output_values) = output_sample.as_array_mut() else {
        return false;
    };
    let Some((left_value, right_value)) = metric_binary_operand_values(operands) else {
        return false;
    };
    let scalar_operands = ScalarOperands {
        sample: left_value,
        scalar: right_value,
        scalar_side: ScalarSide::Right,
    };
    let Some(result) = metric_scalar_arithmetic_value(scalar_operands, op) else {
        return false;
    };
    if let Some(value) = output_values.get_mut(1) {
        *value = json!(format_metric_value(result));
    }
    true
}
