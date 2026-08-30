use super::{MetricQuery, ComparisonOp, MetricVectorMatching};

#[derive(Clone, Debug, PartialEq)]
pub struct MetricBinaryComparison {
    pub left: MetricQuery,
    pub op: ComparisonOp,
    pub bool_modifier: bool,
    pub matching: Option<MetricVectorMatching>,
    pub right: MetricQuery,
}
