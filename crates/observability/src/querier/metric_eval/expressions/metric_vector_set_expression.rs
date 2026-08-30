use super::{MetricBinarySetOp, MetricVectorMatching};

pub(crate) struct MetricVectorSetExpression {
    pub(crate) metric_query: String,
    pub(crate) vector_query: String,
    pub(crate) vector_on_left: bool,
    pub(crate) op: MetricBinarySetOp,
    pub(crate) matching: Option<MetricVectorMatching>,
}
