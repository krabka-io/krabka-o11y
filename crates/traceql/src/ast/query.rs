use super::{Pipeline, QueryHints, SpansetExpr};

#[derive(Clone, Debug, PartialEq)]
/// A parsed `TraceQL` spanset expression, pipeline, and hint set.
pub struct Query {
    /// The spanset selection expression.
    pub root: SpansetExpr,
    /// The ordered pipeline operations.
    pub pipeline: Vec<Pipeline>,
    /// Optional execution hints from the query.
    pub hints: QueryHints,
}
