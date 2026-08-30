use super::*;

pub(crate) struct MetricVectorArithmeticExpression {
    pub(crate) metric_query: String,
    pub(crate) vector_query: String,
    pub(crate) vector_on_left: bool,
    pub(crate) op: MetricScalarArithmeticOp,
    pub(crate) matching: Option<MetricVectorMatching>,
}
