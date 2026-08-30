use super::MetricScalarArithmeticOp;

pub(crate) fn parse_metric_arithmetic_operator(operator: &str) -> Option<MetricScalarArithmeticOp> {
    match operator {
        "+" => Some(MetricScalarArithmeticOp::Add),
        "-" => Some(MetricScalarArithmeticOp::Subtract),
        "*" => Some(MetricScalarArithmeticOp::Multiply),
        "/" => Some(MetricScalarArithmeticOp::Divide),
        "%" => Some(MetricScalarArithmeticOp::Modulo),
        "^" => Some(MetricScalarArithmeticOp::Power),
        _ => None,
    }
}
