use super::*;

pub(crate) fn metric_scalar_arithmetic_value(
    sample: MetricValue,
    op: MetricScalarArithmeticOp,
    scalar: MetricValue,
    scalar_on_left: bool,
) -> Option<MetricValue> {
    let (left, right) = if scalar_on_left {
        (scalar, sample)
    } else {
        (sample, scalar)
    };
    match op {
        MetricScalarArithmeticOp::Add => Some(left.add(right)),
        MetricScalarArithmeticOp::Subtract => Some(left.subtract(right)),
        MetricScalarArithmeticOp::Multiply => Some(left.multiply(right)),
        MetricScalarArithmeticOp::Divide => left.divide(right),
        MetricScalarArithmeticOp::Modulo => left.modulo(right),
        MetricScalarArithmeticOp::Power => left.power(right),
    }
}
