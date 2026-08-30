use async_trait::async_trait;
use krabka_units::prelude::*;

use super::{
    FrontendRangeQuery, FrontendRangeRequest, MomentReduction, QueryShardExecution,
    QueryShardReducer,
    cache::RangeQueryCache,
    merge::{
        divide_range_query_results, merge_range_query_results_with_reducer,
        reduce_moment_range_query_results, reduce_rank_range_query_results,
    },
    plan::{plan_range_query, query_shard_execution, query_with_shard_selector},
};
use crate::{MetricStore, PromqlEngine, PromqlError, QueryResult};

// === split-modules: generated submodules ===
mod execute_avg_range_query_frontend;
mod execute_moment_range_query_frontend;
mod execute_planned_range_queries;
mod execute_range_query_frontend;
mod execute_single_range_query;
mod promql_engine;
mod range_query_executor;

use execute_avg_range_query_frontend::execute_avg_range_query_frontend;
use execute_moment_range_query_frontend::execute_moment_range_query_frontend;
pub (super) use execute_planned_range_queries::execute_planned_range_queries;
pub use execute_range_query_frontend::execute_range_query_frontend;
use execute_single_range_query::execute_single_range_query;
pub use range_query_executor::RangeQueryExecutor;
