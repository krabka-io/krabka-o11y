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
mod matrix_selector_at;
#[cfg(test)]
use matrix_selector_at::MatrixSelectorAt;
mod histogram;
mod histogram_plan;
mod histogram_stats_scope;
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
mod scalar;
mod scalar_eval;
mod selector;
mod selector_eval;
mod selector_plan;
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
pub(crate) use annotations::emit_warning;
#[cfg(test)]
use annotations::{invalid_quantile_warning, is_valid_quantile};
#[cfg(test)]
use binary::{InstantOperands, InstantValue, combine_instant_binary};
use futures::{FutureExt, future::BoxFuture};
pub(crate) use histogram::add_compatible_native_histogram;
#[cfg(all(test, feature = "experimental-functions"))]
use histogram::apply_histogram_quantiles;
#[cfg(test)]
use histogram::{
    FractionBounds, HistogramAccessor, apply_histogram_accessor, apply_histogram_fraction,
    apply_histogram_quantile,
};
use histogram::{
    native_histograms_are_range_compatible, reconcile_native_histogram_layouts,
    scale_native_histogram_values,
};
use histogram_stats_scope::{histogram_stats_enabled, with_histogram_stats};
#[cfg(test)]
use info::apply_info;
use krabka_units::prelude::*;
use planned::{InstantShape, PlannedInstant};
use planner_support::{LabelOpsKind, string_literal_value};
#[cfg(test)]
use planner_support::{match_rate_range_call, range_expr_routes_through_planner};
use promql_parser::parser::{Expr, VectorSelector};
pub(crate) use query_stats::{
    PerStepSampleStats, QuerySampleStats, collect_query_sample_stats, query_stats_step,
};
use query_stats::{query_stats_enabled, record_queryable_samples};
pub(crate) use range_functions::kahan_sum_inc;
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
#[cfg(test)]
use crate::planner::label_ops;
use crate::{
    PromqlError,
    error::Result,
    planner::ExtendedSelectorModifier,
    result::{InstantSample, RangeSeries},
    store::MetricStore,
};

#[derive(Clone, Copy)]
pub(super) struct InnerVectorQuery<'a> {
    tenant: &'a str,
    value_arg: &'a Expr,
    time_ms: i64,
}

impl<'a> InnerVectorQuery<'a> {
    pub(super) fn new(tenant: &'a str, value_arg: &'a Expr, time_ms: i64) -> Self {
        Self {
            tenant,
            value_arg,
            time_ms,
        }
    }
}

impl<S: MetricStore> PromqlEngine<S> {
    pub(super) fn resolve_inner_vector<'a>(
        &'a self,
        query: InnerVectorQuery<'a>,
        select: impl FnOnce(
            &'a Self,
            &'a str,
            &'a VectorSelector,
            i64,
        ) -> BoxFuture<'a, Result<Option<Vec<InstantSample>>>>
        + Send
        + 'a,
    ) -> BoxFuture<'a, Result<Option<Vec<InstantSample>>>> {
        async move {
            let inner = unparenthesized_expression(query.value_arg);
            if let Expr::VectorSelector(selector) = inner {
                return select(self, query.tenant, selector, query.time_ms).await;
            }
            self.planned_instant_vector(query.tenant, inner, query.time_ms)
                .await
        }
        .boxed()
    }
}

fn unparenthesized_expression(mut expression: &Expr) -> &Expr {
    while let Expr::Paren(paren) = expression {
        expression = &paren.expr;
    }
    expression
}

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

mod promql_engine;
mod query_limits;

pub(crate) use histogram::{
    compact_spanned_histogram_counts, for_each_spanned_histogram_count, spanned_histogram_counts,
    standard_histogram_bound, template_histogram_value,
};
pub(crate) use labels::aggregate_labels;
pub use promql_engine::PromqlEngine;
use query_limits::{samples_per_query_exceeded, series_per_query_exceeded};

pub(crate) fn range_resolution_exceeds_limit(start_ms: i64, end_ms: i64, step_ms: i64) -> bool {
    step_ms > 0
        && end_ms.saturating_sub(start_ms) / step_ms
            > i64::try_from(MAX_RESOLUTION_POINTS).unwrap_or(i64::MAX)
}

pub(crate) use range_functions::over_time_mean;

/// Maximum number of resolution points in one range or subquery series.
pub const MAX_RESOLUTION_POINTS: u64 = 11_000;

/// Static options for `PromQL` evaluation.
///
/// This type is not `Eq`, because the two windows are [`Time`] quantities
/// that store `f64`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct EngineOpts {
    /// Maximum age of a sample considered by an instant-vector selector.
    pub lookback_delta: Time,
    /// Global evaluation interval used when a subquery omits its resolution.
    pub eval_interval: Time,
    /// Check counter metadata instead of conventional metric-name suffixes.
    pub enable_type_and_unit_labels: bool,
    /// Maximum float samples returned by one query.
    pub max_samples: usize,
    /// Maximum series one query may select. `0` turns the cap off.
    pub max_fetched_series: usize,
}

impl Default for EngineOpts {
    fn default() -> Self {
        Self {
            lookback_delta: minutes(5),
            eval_interval: minutes(1),
            enable_type_and_unit_labels: true,
            max_samples: 50_000_000,
            max_fetched_series: 0,
        }
    }
}

/// Returns the active query bounds for `@ start()` and `@ end()` resolution.
pub(crate) fn current_at_modifier_bounds() -> Option<AtModifierBounds> {
    AT_MODIFIER_BOUNDS.try_with(|bounds| *bounds).ok()
}

/// Returns the resolution-point count `(end_ms - start_ms) / step + 1`.
///
/// # Errors
/// Returns [`PromqlError::Plan`] when `step` is not positive or the interval
/// count is greater than [`MAX_RESOLUTION_POINTS`].
pub fn check_resolution_points(start_ms: i64, end_ms: i64, step: Time) -> Result<u64> {
    let step_ms = step.millis_i64();
    if step_ms <= 0 {
        return Err(PromqlError::Plan(format!(
            "zero or negative query resolution step widths are not accepted. Try a positive integer (step={step_ms}ms)"
        )));
    }
    let span = u64::try_from(end_ms.saturating_sub(start_ms).max(0)).unwrap_or(u64::MAX);
    let step = u64::try_from(step_ms).unwrap_or(u64::MAX);
    let intervals = span / step;
    if intervals > MAX_RESOLUTION_POINTS {
        return Err(PromqlError::Plan(
            "exceeded maximum resolution of 11,000 points per timeseries. \
             Try decreasing the query resolution (?step=XX)"
                .to_string(),
        ));
    }
    Ok(intervals.saturating_add(1))
}

#[cfg(feature = "experimental-functions")]
#[derive(Clone, Copy)]
pub(crate) struct QueryRangeContext {
    pub(crate) start_ms: i64,
    pub(crate) end_ms: i64,
    pub(crate) step: Time,
}

pub(crate) struct RangeEval {
    pub(crate) series: Vec<RangeSeries>,
    pub(crate) window: RangeWindow,
}

#[derive(Clone, Copy)]
pub(crate) struct RangeWindow {
    pub(crate) end_ms: i64,
    pub(crate) range: Time,
    pub(crate) modifier: Option<ExtendedSelectorModifier>,
    pub(crate) type_and_unit_labels: annotations::TypeAndUnitLabels,
}
