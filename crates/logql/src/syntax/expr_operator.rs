use super::{MetricScalarArithmeticOp, ComparisonOp, MetricBinarySetOp};

#[derive(Clone, Copy)]
pub(crate) enum ExprOperator {
    Arithmetic(MetricScalarArithmeticOp),
    Comparison(ComparisonOp),
    Set(MetricBinarySetOp),
}
