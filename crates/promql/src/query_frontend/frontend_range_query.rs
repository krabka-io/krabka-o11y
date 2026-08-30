use super::*;

/// One subquery the query-frontend can fan out to a querier.
///
/// Not `Eq`: `step` is a [`Time`], which stores `f64`.
#[derive(Clone, Debug, PartialEq)]
pub struct FrontendRangeQuery {
    pub query: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub step: Time,
    pub shard: Option<QueryShard>,
}

impl FrontendRangeQuery {
    #[must_use]
    pub fn shard_matcher(&self) -> Option<LabelMatcher> {
        self.shard.map(QueryShard::matcher)
    }
}
