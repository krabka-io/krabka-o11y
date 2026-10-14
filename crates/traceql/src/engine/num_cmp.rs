use super::{ComparisonOp, float_cmp::cmp_ordering};

pub(crate) fn num_cmp(value: i64, op: ComparisonOp, rhs: i64) -> bool {
    cmp_ordering(Some(value.cmp(&rhs)), op)
}
