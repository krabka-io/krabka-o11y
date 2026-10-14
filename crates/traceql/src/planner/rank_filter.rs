use super::ComparisonOp;

#[derive(Clone, Copy)]
pub(crate) struct RankFilter {
    pub(crate) op: ComparisonOp,
    pub(crate) value: f64,
}

impl RankFilter {
    pub(crate) fn new(op: ComparisonOp, value: f64) -> Self {
        Self { op, value }
    }
}
