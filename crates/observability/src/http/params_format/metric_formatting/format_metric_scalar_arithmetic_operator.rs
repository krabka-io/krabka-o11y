use super::MetricScalarArithmeticOp;

pub(crate) fn format_metric_scalar_arithmetic_operator(
    op: MetricScalarArithmeticOp,
) -> &'static str {
    krabka_logql::arithmetic_text(op)
}
