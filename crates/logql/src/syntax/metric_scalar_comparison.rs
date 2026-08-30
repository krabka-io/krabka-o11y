use super::{MetricQuery, ComparisonOp};

#[derive(Clone, Debug, PartialEq)]
pub struct MetricScalarComparison {
    pub query: MetricQuery,
    pub op: ComparisonOp,
    pub bool_modifier: bool,
    pub scalar: String,
    pub scalar_on_left: bool,
}
