use super::*;

pub(crate) fn format_metric_scalar_arithmetic_operator(
    op: MetricScalarArithmeticOp,
) -> &'static str {
    match op {
        MetricScalarArithmeticOp::Add => "+",
        MetricScalarArithmeticOp::Subtract => "-",
        MetricScalarArithmeticOp::Multiply => "*",
        MetricScalarArithmeticOp::Divide => "/",
        MetricScalarArithmeticOp::Modulo => "%",
        MetricScalarArithmeticOp::Power => "^",
    }
}
