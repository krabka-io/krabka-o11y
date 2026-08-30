use super::{ComparisonOp, MetricValue, Ordering};

pub(crate) fn metric_scalar_comparison_matches(
    sample: MetricValue,
    op: ComparisonOp,
    scalar: MetricValue,
    scalar_on_left: bool,
) -> bool {
    let (left, right) = if scalar_on_left {
        (scalar, sample)
    } else {
        (sample, scalar)
    };
    let ordering = left.cmp_value(right);
    match op {
        ComparisonOp::Equal => ordering == Ordering::Equal,
        ComparisonOp::NotEqual => ordering != Ordering::Equal,
        ComparisonOp::RegexEqual | ComparisonOp::RegexNotEqual => false,
        ComparisonOp::Greater => ordering == Ordering::Greater,
        ComparisonOp::GreaterEqual => matches!(ordering, Ordering::Greater | Ordering::Equal),
        ComparisonOp::Less => ordering == Ordering::Less,
        ComparisonOp::LessEqual => matches!(ordering, Ordering::Less | Ordering::Equal),
    }
}
