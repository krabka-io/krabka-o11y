use super::*;

#[derive(Clone, Copy)]
pub(crate) enum ExprOperator {
    Arithmetic(MetricScalarArithmeticOp),
    Comparison(ComparisonOp),
    Set(MetricBinarySetOp),
}
