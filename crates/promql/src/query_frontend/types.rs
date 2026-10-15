use krabka_query_frontend::AdmissionLimits;

use super::{LabelMatcher, MatchOp, QUERY_SHARD_LABEL, TenantId, Time};

/// Query-frontend range splitting and sharding options.
///
/// Not `Eq`: `split_interval` is a [`Time`], which stores `f64`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct QueryFrontendOptions {
    /// Width of the absolute window that each sub-range is split on.
    pub split_interval: Time,
    pub shard_count: usize,
}

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

/// One user range query that enters the query-frontend.
#[derive(Clone, Debug, PartialEq)]
pub struct FrontendRangeRequest {
    pub tenant: TenantId,
    pub query: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub step: Time,
    pub opts: QueryFrontendOptions,
    pub admission_limits: AdmissionLimits,
}

/// One Mimir-compatible query shard.
///
/// Shards are one-based on the wire: `1_of_3`, `2_of_3`, ...
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct QueryShard {
    pub index: usize,
    pub total: usize,
}

impl QueryShard {
    #[must_use]
    pub fn selector_value(self) -> String {
        format!("{}_of_{}", self.index, self.total)
    }

    #[must_use]
    pub fn matcher(self) -> LabelMatcher {
        LabelMatcher {
            name: QUERY_SHARD_LABEL.to_string(),
            op: MatchOp::Eq,
            value: self.selector_value().into(),
        }
    }
}
