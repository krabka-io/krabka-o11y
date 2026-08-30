use super::{ComparisonOp, MetricBinarySetOp, MetricScalarArithmeticOp};

#[derive(Clone, Copy)]
pub(crate) enum ExprOperator {
    Arithmetic(MetricScalarArithmeticOp),
    Comparison(ComparisonOp),
    Set(MetricBinarySetOp),
}
