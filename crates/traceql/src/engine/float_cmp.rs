use super::ComparisonOp;

pub(crate) fn float_cmp(value: f64, op: ComparisonOp, rhs: f64) -> bool {
    cmp_ordering(value.partial_cmp(&rhs), op)
}

pub(crate) fn cmp_ordering(ordering: Option<std::cmp::Ordering>, op: ComparisonOp) -> bool {
    match op {
        ComparisonOp::Eq => ordering == Some(std::cmp::Ordering::Equal),
        ComparisonOp::Neq => ordering != Some(std::cmp::Ordering::Equal),
        ComparisonOp::Lt => ordering == Some(std::cmp::Ordering::Less),
        ComparisonOp::Lte => matches!(
            ordering,
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        ),
        ComparisonOp::Gt => ordering == Some(std::cmp::Ordering::Greater),
        ComparisonOp::Gte => matches!(
            ordering,
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        ),
        ComparisonOp::Re | ComparisonOp::Nre => false,
    }
}
