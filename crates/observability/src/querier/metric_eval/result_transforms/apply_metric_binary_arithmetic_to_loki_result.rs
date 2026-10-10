use super::{
    MetricBinaryOperator, MetricScalarArithmeticOp, MetricVectorMatching, Value,
    apply_metric_binary_arithmetic_to_series,
    apply_metric_binary_arithmetic_to_series_with_left_operand,
};

pub(crate) fn apply_metric_binary_arithmetic_to_loki_result(
    left: &mut Value,
    right: &Value,
    op: MetricScalarArithmeticOp,
    matching: Option<&MetricVectorMatching>,
) {
    MetricBinaryOperator {
        matching,
        apply_series: |left_series: &mut Value, right_series: &Value| {
            apply_metric_binary_arithmetic_to_series(left_series, right_series, op)
        },
        apply_series_with_left_operand: |output_series: &mut Value, left_series: &Value| {
            apply_metric_binary_arithmetic_to_series_with_left_operand(
                output_series,
                left_series,
                op,
            )
        },
    }
    .apply_to_loki_result(left, right);
}
