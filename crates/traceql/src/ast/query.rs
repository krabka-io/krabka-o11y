use super::*;

#[derive(Clone, Debug, PartialEq)]
pub struct Query {
    pub root: SpansetExpr,
    pub pipeline: Vec<Pipeline>,
    pub hints: QueryHints,
}
