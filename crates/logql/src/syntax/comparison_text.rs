use super::ComparisonOp;

pub(crate) fn comparison_text(op: ComparisonOp) -> &'static str {
    match op {
        ComparisonOp::Equal => "==",
        ComparisonOp::NotEqual => "!=",
        ComparisonOp::Greater => ">",
        ComparisonOp::GreaterEqual => ">=",
        ComparisonOp::Less => "<",
        ComparisonOp::LessEqual => "<=",
        ComparisonOp::RegexEqual => "=~",
        ComparisonOp::RegexNotEqual => "!~",
    }
}
