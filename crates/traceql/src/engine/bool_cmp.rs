use super::ComparisonOp;

pub(crate) fn bool_cmp(value: bool, op: ComparisonOp, rhs: bool) -> bool {
    match op {
        ComparisonOp::Eq => value == rhs,
        ComparisonOp::Neq => value != rhs,
        _ => false,
    }
}
