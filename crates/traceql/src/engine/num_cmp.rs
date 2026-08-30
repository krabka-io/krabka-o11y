use super::ComparisonOp;

pub(crate) fn num_cmp(value: i64, op: ComparisonOp, rhs: i64) -> bool {
    match op {
        ComparisonOp::Eq => value == rhs,
        ComparisonOp::Neq => value != rhs,
        ComparisonOp::Lt => value < rhs,
        ComparisonOp::Lte => value <= rhs,
        ComparisonOp::Gt => value > rhs,
        ComparisonOp::Gte => value >= rhs,
        ComparisonOp::Re | ComparisonOp::Nre => false,
    }
}
