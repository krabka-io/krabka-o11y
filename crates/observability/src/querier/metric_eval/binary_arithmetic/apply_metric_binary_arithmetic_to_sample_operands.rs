use super::{
    MetricScalarArithmeticOp, SampleOperands, Value, format_metric_value, json,
    metric_scalar_arithmetic_value, output_sample_scalar_operands,
};

pub(crate) fn apply_metric_binary_arithmetic_to_sample_operands(
    output_sample: &mut Value,
    operands: SampleOperands<'_>,
    op: MetricScalarArithmeticOp,
) -> bool {
    let Some((output_values, scalar_operands)) =
        output_sample_scalar_operands(output_sample, operands)
    else {
        return false;
    };
    let Some(result) = metric_scalar_arithmetic_value(scalar_operands, op) else {
        return false;
    };
    if let Some(value) = output_values.get_mut(1) {
        *value = json!(format_metric_value(result));
    }
    true
}
