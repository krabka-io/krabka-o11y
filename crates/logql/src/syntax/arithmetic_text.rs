use super::MetricScalarArithmeticOp;

pub(crate) fn arithmetic_text(op: MetricScalarArithmeticOp) -> &'static str {
    match op {
        MetricScalarArithmeticOp::Add => "+",
        MetricScalarArithmeticOp::Subtract => "-",
        MetricScalarArithmeticOp::Multiply => "*",
        MetricScalarArithmeticOp::Divide => "/",
        MetricScalarArithmeticOp::Modulo => "%",
        MetricScalarArithmeticOp::Power => "^",
    }
}
