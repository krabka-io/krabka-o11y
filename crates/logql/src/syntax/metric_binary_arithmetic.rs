use super::{MetricQuery, MetricScalarArithmeticOp, MetricVectorMatching};

#[derive(Clone, Debug, PartialEq)]
pub struct MetricBinaryArithmetic {
    pub left: MetricQuery,
    pub op: MetricScalarArithmeticOp,
    pub matching: Option<MetricVectorMatching>,
    pub right: MetricQuery,
}
