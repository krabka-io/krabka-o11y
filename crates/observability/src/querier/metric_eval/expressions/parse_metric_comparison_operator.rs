use super::ComparisonOp;

pub(crate) fn parse_metric_comparison_operator(operator: &str) -> Option<ComparisonOp> {
    match operator {
        "==" => Some(ComparisonOp::Equal),
        "!=" => Some(ComparisonOp::NotEqual),
        ">" => Some(ComparisonOp::Greater),
        ">=" => Some(ComparisonOp::GreaterEqual),
        "<" => Some(ComparisonOp::Less),
        "<=" => Some(ComparisonOp::LessEqual),
        _ => None,
    }
}
