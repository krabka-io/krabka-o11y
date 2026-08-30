//! Query-frontend range splitting, sharding, and merge helpers.

pub use krabka_blockstore::QUERY_SHARD_LABEL;
use krabka_blockstore::{LabelMatcher, MatchOp};
use krabka_units::prelude::*;
use promql_parser::parser::LabelModifier;

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
use merge::merge_range_query_results_with_reducer;
pub use plan::plan_range_query;
#[cfg(test)]
use plan::query_with_shard_selector;

#[cfg(test)]
use crate::PromqlError;

#[cfg(test)]
mod tests;

mod frontend_range_query;
mod frontend_range_request;
mod moment_reduction;
mod query_frontend_options;
mod query_shard;
mod query_shard_execution;
mod query_shard_reducer;
mod rank_reduction;

pub use frontend_range_query::FrontendRangeQuery;
pub use frontend_range_request::FrontendRangeRequest;
use moment_reduction::MomentReduction;
pub use query_frontend_options::QueryFrontendOptions;
pub use query_shard::QueryShard;
use query_shard_execution::QueryShardExecution;
use query_shard_reducer::QueryShardReducer;
use rank_reduction::RankReduction;
