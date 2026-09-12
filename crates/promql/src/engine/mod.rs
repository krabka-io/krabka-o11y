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
mod grid_leaf;
mod histogram;
mod histogram_plan;
mod info;
mod info_plan;
mod instant_query;
mod labels;
mod merge_by_fingerprint;
mod planned;
mod planner_dispatch;
mod planner_support;
mod query_stats;
mod range_fold_plan;
mod range_functions;
mod range_query;
mod result_utils;
mod row_cache;
mod samples_per_query_exceeded;
mod scalar;
mod scalar_eval;
mod selector;
mod selector_eval;
mod selector_plan;
mod series_per_query_exceeded;
mod step_vectors;
mod store_scans;
#[cfg(test)]
mod test_oracle;
mod util_plan;
mod vector_transform_plan;

use std::sync::Arc;

#[cfg(test)]
use aggregation::{
    AggregateOp, apply_count_values_aggregate, apply_k_aggregate, apply_quantile_aggregate,
    apply_simple_aggregate,
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
pub(crate) use query_stats::{QuerySampleStats, collect_query_sample_stats, query_stats_step};
use query_stats::{query_stats_enabled, record_queryable_samples};
#[cfg(all(test, feature = "experimental-functions"))]
use range_functions::validate_smoothing_factor;
#[cfg(test)]
use range_functions::{IrateFn, OverTimeFn, RangeFn};
use range_functions::{OuterRangeFn, apply_outer_range_fn};
use samples_per_query_exceeded::samples_per_query_exceeded;
pub(crate) use selector::label_matcher_sets;
use selector::{AtModifierBounds, apply_selector_time_modifier, selector_duration};
use series_per_query_exceeded::series_per_query_exceeded;

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
    /// The `[start, end]` bounds of the active query. A range query scopes its
    /// grid bounds; an instant query scopes `[time, time]`. Selectors with an
    /// `@ start()` or `@ end()` modifier resolve against these bounds.
    static AT_MODIFIER_BOUNDS: AtModifierBounds;
}

#[cfg(test)]
mod tests;

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
