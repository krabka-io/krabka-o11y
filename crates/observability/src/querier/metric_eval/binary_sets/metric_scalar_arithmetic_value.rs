use super::{MetricScalarArithmeticOp, MetricValue, ScalarOperands};

pub(crate) fn metric_scalar_arithmetic_value(
    operands: ScalarOperands,
    op: MetricScalarArithmeticOp,
) -> Option<MetricValue> {
    let (left, right) = operands.left_and_right();
    match op {
        MetricScalarArithmeticOp::Add => Some(left.add(right)),
        MetricScalarArithmeticOp::Subtract => Some(left.subtract(right)),
        MetricScalarArithmeticOp::Multiply => Some(left.multiply(right)),
        MetricScalarArithmeticOp::Divide => left.divide(right),
        MetricScalarArithmeticOp::Modulo => left.modulo(right),
        MetricScalarArithmeticOp::Power => left.power(right),
    }
}
