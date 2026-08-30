use super::{VectorAggregationOp, VectorGrouping};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorAggregation {
    pub op: VectorAggregationOp,
    pub grouping: Option<VectorGrouping>,
}
