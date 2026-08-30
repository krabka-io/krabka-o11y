use super::{MetricBinarySetOp, MetricQuery, MetricVectorMatching};

#[derive(Clone, Debug, PartialEq)]
pub struct MetricBinarySet {
    pub left: MetricQuery,
    pub op: MetricBinarySetOp,
    pub matching: Option<MetricVectorMatching>,
    pub right: MetricQuery,
}
