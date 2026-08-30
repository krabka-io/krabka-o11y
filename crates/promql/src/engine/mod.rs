//! Minimal `PromQL` engine entry point.
//!
//! This module implements selector evaluation over the `MetricStore` contract.
//! The rest of the Slice 2 planner (functions, aggregations, binary ops) will
//! build on this public API.

mod aggregate_plan;
mod aggregation;
mod annotations;
mod assembly;
mod binary;
mod binary_plan;
mod execution;
mod histogram;
mod histogram_plan;
mod info;
mod info_plan;
mod instant_query;
mod labels;
mod planned;
mod planner_dispatch;
mod planner_support;
mod range_fold_plan;
mod range_functions;
mod range_query;
mod result_utils;
mod row_cache;
mod scalar;
mod scalar_eval;
mod selector;
mod selector_eval;
mod selector_plan;
mod store_scans;
#[cfg(test)]
mod test_oracle;
mod util_plan;
mod vector_transform_plan;

use std::sync::Arc;

#[cfg(test)]
use aggregation::{
    AggregateOp, aggregate_k, aggregate_quantile, apply_count_values_aggregate, apply_k_aggregate,
    apply_quantile_aggregate, apply_simple_aggregate,
};
#[cfg(feature = "experimental-functions")]
#[cfg(test)]
use aggregation::{apply_limit_ratio_aggregate, apply_limitk_aggregate};
#[cfg(test)]
pub(crate) use annotations::ANNOTATIONS;
#[cfg(test)]
use annotations::{emit_warning, invalid_quantile_warning, is_valid_quantile};
#[cfg(test)]
use binary::{InstantValue, combine_instant_binary};
pub(crate) use histogram::add_compatible_native_histogram;
#[cfg(all(test, feature = "experimental-functions"))]
use histogram::apply_histogram_quantiles;
#[cfg(test)]
use histogram::{
    HistogramAccessor, apply_histogram_accessor, apply_histogram_fraction, apply_histogram_quantile,
};
use histogram::{native_histograms_are_range_compatible, scale_native_histogram_values};
#[cfg(test)]
use info::apply_info;
use krabka_units::prelude::*;
use planned::{InstantShape, PlannedInstant};
use planner_support::{LabelOpsKind, string_literal_value};
#[cfg(test)]
use planner_support::{match_rate_range_call, range_expr_routes_through_planner};
#[cfg(all(test, feature = "experimental-functions"))]
use range_functions::validate_smoothing_factor;
#[cfg(test)]
use range_functions::{IrateFn, OverTimeFn, RangeFn};
use range_functions::{OuterRangeFn, apply_outer_range_fn};
pub(crate) use selector::label_matcher_sets;
use selector::{AtModifierBounds, apply_selector_time_modifier, selector_duration};

#[cfg(test)]
use crate::extension::is_stale_nan;
#[cfg(test)]
use crate::planner::ExtendedSelectorExpr;
#[cfg(test)]
use crate::planner::label_ops;
use crate::{
    PromqlError, error::Result, planner::ExtendedSelectorModifier, result::RangeSeries,
    store::MetricStore,
};

#[cfg(feature = "experimental-functions")]
tokio::task_local! {
    pub(super) static QUERY_RANGE_CONTEXT: QueryRangeContext;
}

tokio::task_local! {
    /// The `[start, end]` bounds of the active range query. The per-step planner
    /// range driver ([`PromqlEngine::eval_range_via_planner_scoped`]) scopes
    /// them. A bare top-level selector with an `@ start()` or `@ end()` modifier
    /// then resolves those bounds to the range bounds of the query, as
    /// Prometheus does, and the planner still evaluates the selector at each grid
    /// step. This task-local is absent for an instant query. There, `@ start()`
    /// and `@ end()` are invalid, and the selector planner raises the same hard
    /// error as the interpreter.
    static AT_MODIFIER_BOUNDS: AtModifierBounds;
}

#[cfg(test)]
mod tests;

// === split-modules: generated submodules ===
mod check_resolution_points;
mod current_at_modifier_bounds;
mod engine_opts;
mod max_resolution_points;
mod promql_engine;
mod query_range_context;
mod range_eval;

pub use check_resolution_points::check_resolution_points;
use current_at_modifier_bounds::current_at_modifier_bounds;
pub use engine_opts::EngineOpts;
pub use max_resolution_points::MAX_RESOLUTION_POINTS;
pub use promql_engine::PromqlEngine;
#[cfg(feature = "experimental-functions")]
pub(super) use query_range_context::QueryRangeContext;
use range_eval::RangeEval;
