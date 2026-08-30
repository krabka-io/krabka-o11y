use super::ComparisonOp;

pub(crate) fn format_metric_scalar_comparison_operator(op: ComparisonOp) -> Option<&'static str> {
    match op {
        ComparisonOp::Equal => Some("=="),
        ComparisonOp::NotEqual => Some("!="),
        ComparisonOp::Greater => Some(">"),
        ComparisonOp::GreaterEqual => Some(">="),
        ComparisonOp::Less => Some("<"),
        ComparisonOp::LessEqual => Some("<="),
        ComparisonOp::RegexEqual | ComparisonOp::RegexNotEqual => None,
    }
}
