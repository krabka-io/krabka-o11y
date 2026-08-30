use super::{ComparisonOp, MetricVectorMatching};

pub(crate) struct MetricVectorComparisonExpression {
    pub(crate) metric_query: String,
    pub(crate) vector_query: String,
    pub(crate) vector_on_left: bool,
    pub(crate) op: ComparisonOp,
    pub(crate) bool_modifier: bool,
    pub(crate) matching: Option<MetricVectorMatching>,
}
