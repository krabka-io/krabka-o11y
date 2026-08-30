use krabka_blockstore::QUERY_SHARD_LABEL;
use krabka_units::prelude::*;
use promql_parser::{
    label as prom_label,
    parser::{
        AggregateExpr, Expr, LabelModifier, VectorSelector,
        token::{
            T_AVG, T_BOTTOMK, T_COUNT, T_GROUP, T_MAX, T_MIN, T_STDDEV, T_STDVAR, T_SUM, T_TOPK,
            TokenType,
        },
    },
};

use super::{
    FrontendRangeQuery, MomentReduction, QueryFrontendOptions, QueryShard, QueryShardExecution,
    QueryShardReducer, RankReduction,
};
use crate::{PromqlError, engine::MAX_RESOLUTION_POINTS, parse_promql};

// === split-modules: generated submodules ===
mod absolute_split_window;
mod aggregate_k;
mod avg_partial_queries;
mod check_range_resolution;
mod expr_contains_aggregate;
mod expr_shard_reducer;
mod expr_supports_frontend_sharding;
mod inject_shard_into_expr;
mod inject_shard_into_selector;
mod moment_partial_queries;
mod plan_range_query;
mod push_sharded_subqueries;
mod query_shard_execution;
mod query_supports_frontend_sharding;
mod query_with_shard_selector;
mod rank_reduction;

use absolute_split_window::absolute_split_window;
use aggregate_k::aggregate_k;
use avg_partial_queries::avg_partial_queries;
use check_range_resolution::check_range_resolution;
use expr_contains_aggregate::expr_contains_aggregate;
use expr_shard_reducer::expr_shard_reducer;
use expr_supports_frontend_sharding::expr_supports_frontend_sharding;
use inject_shard_into_expr::inject_shard_into_expr;
use inject_shard_into_selector::inject_shard_into_selector;
use moment_partial_queries::moment_partial_queries;
pub use plan_range_query::plan_range_query;
use push_sharded_subqueries::push_sharded_subqueries;
pub (super) use query_shard_execution::query_shard_execution;
use query_supports_frontend_sharding::query_supports_frontend_sharding;
pub (super) use query_with_shard_selector::query_with_shard_selector;
use rank_reduction::rank_reduction;
