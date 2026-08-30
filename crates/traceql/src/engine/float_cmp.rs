use super::ComparisonOp;

pub(crate) fn float_cmp(value: f64, op: ComparisonOp, rhs: f64) -> bool {
    let ordering = value.partial_cmp(&rhs);
    match op {
        ComparisonOp::Eq => ordering == Some(std::cmp::Ordering::Equal),
        ComparisonOp::Neq => ordering != Some(std::cmp::Ordering::Equal),
        ComparisonOp::Lt => value < rhs,
        ComparisonOp::Lte => value <= rhs,
        ComparisonOp::Gt => value > rhs,
        ComparisonOp::Gte => value >= rhs,
        ComparisonOp::Re | ComparisonOp::Nre => false,
    }
}
