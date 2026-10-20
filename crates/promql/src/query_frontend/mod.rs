//! Query-frontend range splitting, sharding, and merge helpers.

pub use krabka_blockstore::QUERY_SHARD_LABEL;
use krabka_blockstore::{MatchOp, TenantId};
use krabka_units::prelude::*;
use promql_parser::parser::LabelModifier;

use crate::PromqlMatcher as LabelMatcher;

mod cache;
mod execution;
mod merge;
mod plan;

#[cfg(test)]
use cache::Clock;
pub use cache::{ObjectStoreQueryFrontendCache, QueryFrontendCache, RangeQueryCache};
#[cfg(test)]
use execution::execute_planned_range_queries;
pub use execution::{RangeQueryExecutor, execute_range_query_frontend};
pub use merge::merge_range_query_results;
#[cfg(test)]
use merge::{merge_range_query_results_with_reducer, reduce_rank_range_query_results};
pub use plan::plan_range_query;
#[cfg(test)]
use plan::query_with_shard_selector;

#[cfg(test)]
use crate::PromqlError;

#[cfg(test)]
mod tests;

mod types;

pub use types::{FrontendRangeQuery, FrontendRangeRequest, QueryFrontendOptions, QueryShard};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MomentReduction {
    Stddev,
    Stdvar,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RankReduction {
    Bottom,
    Top,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueryShardReducer {
    First,
    Sum,
    Min,
    Max,
}

pub(crate) enum QueryShardExecution {
    Merge(QueryShardReducer),
    Avg {
        sum_query: String,
        count_query: String,
    },
    Moments {
        sum_query: String,
        count_query: String,
        sum_squares_query: String,
        kind: MomentReduction,
    },
    Rank {
        k: usize,
        kind: RankReduction,
        modifier: Option<LabelModifier>,
    },
}
