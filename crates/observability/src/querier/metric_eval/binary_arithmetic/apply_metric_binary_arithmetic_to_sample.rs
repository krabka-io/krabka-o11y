use super::{
    MetricScalarArithmeticOp, SampleOperands, Value,
    apply_metric_binary_arithmetic_to_sample_operands,
};

pub(crate) fn apply_metric_binary_arithmetic_to_sample(
    left_sample: &mut Value,
    right_sample: &Value,
    op: MetricScalarArithmeticOp,
) -> bool {
    let original_left = left_sample.clone();
    apply_metric_binary_arithmetic_to_sample_operands(
        left_sample,
        SampleOperands {
            left: &original_left,
            right: right_sample,
        },
        op,
    )
}
