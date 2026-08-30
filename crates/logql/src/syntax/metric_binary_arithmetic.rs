use super::*;

#[derive(Clone, Debug, PartialEq)]
pub struct MetricBinaryArithmetic {
    pub left: MetricQuery,
    pub op: MetricScalarArithmeticOp,
    pub matching: Option<MetricVectorMatching>,
    pub right: MetricQuery,
}
