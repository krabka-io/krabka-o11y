use super::{MetricQuery, MetricScalarArithmeticOp};

#[derive(Clone, Debug, PartialEq)]
pub struct MetricScalarArithmetic {
    pub query: MetricQuery,
    pub op: MetricScalarArithmeticOp,
    pub scalar: String,
    pub scalar_on_left: bool,
}
