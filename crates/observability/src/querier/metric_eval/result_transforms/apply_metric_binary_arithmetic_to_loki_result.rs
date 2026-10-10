use super::{
    MetricBinaryOperator, MetricScalarArithmeticOp, MetricVectorMatching, Value,
    apply_metric_binary_arithmetic_to_series,
    apply_metric_binary_arithmetic_to_series_with_left_operand,
};

/// A vector-to-vector arithmetic operator and its matching modifiers.
#[derive(Clone, Copy)]
pub(crate) struct VectorArithmetic<'a> {
    pub(crate) op: MetricScalarArithmeticOp,
    pub(crate) matching: Option<&'a MetricVectorMatching>,
}

pub(crate) fn apply_metric_binary_arithmetic_to_loki_result(
    left: &mut Value,
    right: &Value,
    vector_arithmetic: VectorArithmetic<'_>,
) {
    let VectorArithmetic { op, matching } = vector_arithmetic;
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
