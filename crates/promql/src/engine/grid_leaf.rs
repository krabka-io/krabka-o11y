//! Whole-grid evaluation of the operator leaves, for the range driver.
//!
//! Each public method here answers one question: what does this leaf evaluate
//! to at the step the driver is on? On the first ask it plans and executes the
//! leaf over the range query's whole grid and memoizes the per-step results in
//! [`RANGE_STEP_VECTORS`]; every later step reads them. Outside a range query,
//! or for a leaf the grid path declines, the method returns `None` and the
//! caller plans that single step as it did before.
//!
//! See [`super::step_vectors`] for why memoizing the leaf's per-step *results*
//! leaves the rest of the engine untouched, and for the scoping and keying
//! rules.

use std::{collections::BTreeMap, sync::Arc};

use krabka_blockstore::{Labels, SeriesFingerprint};
use krabka_units::prelude::*;
use promql_parser::parser::{MatrixSelector, Offset, VectorSelector};

use super::{
    PromqlEngine,
    assembly::{assemble_range_fold_grid, assemble_selector_grid},
    labels::labels_without_metric_name,
    query_stats_enabled,
    selector::{apply_selector_time_modifier, label_matcher_sets, selector_duration},
    step_vectors::{GridVectors, LeafLookup, LeafMemo, RANGE_STEP_VECTORS, StepVectorCache},
};
use crate::{
    error::Result,
    functions::OverTimeFamily,
    planner::{
        StepGrid,
        leaf::{InstantSelectorPlan, plan_instant_vector_selector},
        over_time_range::{
            OVER_TIME_VALUE_COLUMN, OverTimeRangePlan, plan_over_time_range_selector,
        },
        rate_range::{RATE_VALUE_COLUMN, RateRangePlan, RateUdfKind, plan_rate_range_selector},
    },
    result::InstantSample,
    store::MetricStore,
};

mod max_grid_leaf_points;

use max_grid_leaf_points::MAX_GRID_LEAF_POINTS;

impl<S: MetricStore> PromqlEngine<S> {
    /// The memoized value of a bare instant-vector selector at `time_ms`.
    ///
    /// Returns `None` when the leaf is not memoizable at this instant: outside a
    /// range query, at an instant off the query's grid (a subquery's sub-step),
    /// for a selector whose `@` modifier pins its evaluation instant, or for a
    /// leaf over the memo's size budget.
    pub(super) async fn grid_selector_vector(
        &self,
        tenant: &str,
        selector: &VectorSelector,
        time_ms: i64,
    ) -> Result<Option<Vec<InstantSample>>> {
        // Prometheus sample stats intentionally describe each evaluation step,
        // independent of whole-grid execution optimizations.
        if query_stats_enabled() {
            return Ok(None);
        }
        // An `@` modifier pins the evaluation instant, so the leaf is not a
        // function of the grid at all and the driver's per-step path — which
        // resolves the modifier itself — stays in charge.
        if selector.at.is_some() {
            return Ok(None);
        }
        let Some((cache, grid)) = active_step_grid(time_ms) else {
            return Ok(None);
        };
        let key = format!("selector\u{1}{selector}");
        let vectors = match leaf_lookup(&cache, &key, NO_PARAMETER) {
            LeafLookup::Declined => return Ok(None),
            LeafLookup::Ready(vectors) => vectors,
            LeafLookup::Build => {
                let built = self
                    .build_grid_selector(tenant, selector, grid, remaining_budget(&cache))
                    .await?;
                let Some(vectors) = store_leaf(&cache, key, NO_PARAMETER, built) else {
                    return Ok(None);
                };
                vectors
            }
        };
        Ok(vectors.vector_at(time_ms))
    }

    /// The memoized value of a rate-family fold over a matrix selector at
    /// `time_ms`. See [`Self::grid_selector_vector`] for when this returns
    /// `None`.
    pub(super) async fn grid_rate_vector(
        &self,
        tenant: &str,
        selector: &MatrixSelector,
        time_ms: i64,
        kind: RateUdfKind,
    ) -> Result<Option<Vec<InstantSample>>> {
        if query_stats_enabled() {
            return Ok(None);
        }
        if selector.vs.at.is_some() {
            return Ok(None);
        }
        let Some((cache, grid)) = active_step_grid(time_ms) else {
            return Ok(None);
        };
        let key = format!("rate\u{1}{kind:?}\u{1}{selector}");
        let vectors = match leaf_lookup(&cache, &key, NO_PARAMETER) {
            LeafLookup::Declined => return Ok(None),
            LeafLookup::Ready(vectors) => vectors,
            LeafLookup::Build => {
                let built = self
                    .build_grid_rate(tenant, selector, grid, kind, remaining_budget(&cache))
                    .await?;
                let Some(vectors) = store_leaf(&cache, key, NO_PARAMETER, built) else {
                    return Ok(None);
                };
                vectors
            }
        };
        Ok(vectors.vector_at(time_ms))
    }

    /// The memoized value of an `*_over_time` fold over a matrix selector at
    /// `time_ms`. See [`Self::grid_selector_vector`] for when this returns
    /// `None`.
    ///
    /// `phi` is part of the key: `quantile_over_time`'s parameter is resolved
    /// per step, so a step-dependent one must not be served a memo built at a
    /// different value.
    pub(super) async fn grid_over_time_vector(
        &self,
        tenant: &str,
        selector: &MatrixSelector,
        time_ms: i64,
        family: OverTimeFamily,
        phi: f64,
    ) -> Result<Option<Vec<InstantSample>>> {
        if query_stats_enabled() {
            return Ok(None);
        }
        if selector.vs.at.is_some() {
            return Ok(None);
        }
        let Some((cache, grid)) = active_step_grid(time_ms) else {
            return Ok(None);
        };
        let key = format!("over_time\u{1}{family:?}\u{1}{selector}");
        let vectors = match leaf_lookup(&cache, &key, phi.to_bits()) {
            LeafLookup::Declined => return Ok(None),
            LeafLookup::Ready(vectors) => vectors,
            LeafLookup::Build => {
                let built = self
                    .build_grid_over_time(
                        tenant,
                        selector,
                        grid,
                        family,
                        phi,
                        remaining_budget(&cache),
                    )
                    .await?;
                let Some(vectors) = store_leaf(&cache, key, phi.to_bits(), built) else {
                    return Ok(None);
                };
                vectors
            }
        };
        Ok(vectors.vector_at(time_ms))
    }

    /// Plans and executes a bare instant-vector selector over the whole grid.
    ///
    /// The scan window is the union of every step's lookback window, and
    /// `InstantManipulate` makes each step's selection from it — the same
    /// selection the step's own one-point plan would have made, since the
    /// operator picks the latest sample at or before the instant and then
    /// applies the lookback bound.
    async fn build_grid_selector(
        &self,
        tenant: &str,
        selector: &VectorSelector,
        grid: StepGrid,
        budget: usize,
    ) -> Result<Option<GridVectors>> {
        let Some(plan_grid) = shifted_grid(grid, selector.offset.as_ref())? else {
            return Ok(None);
        };
        let lookback_ms = self.opts.lookback_delta.millis_i64();
        let matcher_sets = label_matcher_sets(selector);
        // Stale-NaN markers are kept, exactly as the per-step plan keeps them:
        // `InstantManipulate` drops a step whose selected sample is a marker,
        // which suppresses the series rather than revealing an older sample.
        let series = self
            .labeled_series_sets(
                tenant,
                &matcher_sets,
                plan_grid.start.saturating_sub(lookback_ms),
                plan_grid.end,
                false,
            )
            .await?;
        if over_budget(grid, series.len(), budget) {
            return Ok(None);
        }
        let InstantSelectorPlan {
            ctx,
            plan,
            labels_by_fp,
        } = plan_instant_vector_selector(series, plan_grid, self.opts.lookback_delta).await?;
        let batches = ctx.execute_logical_plan(plan).await?.collect().await?;
        let steps = assemble_selector_grid(&batches, plan_grid)?;
        Ok(Some(GridVectors::new(grid, labels_by_fp, steps, false)))
    }

    /// Plans and executes a rate-family fold over the whole grid.
    async fn build_grid_rate(
        &self,
        tenant: &str,
        selector: &MatrixSelector,
        grid: StepGrid,
        kind: RateUdfKind,
        budget: usize,
    ) -> Result<Option<GridVectors>> {
        let Some(plan_grid) = shifted_grid(grid, selector.vs.offset.as_ref())? else {
            return Ok(None);
        };
        let range = selector_duration(selector.range)?;
        let matcher_sets = label_matcher_sets(&selector.vs);
        let series = self
            .labeled_series_sets(
                tenant,
                &matcher_sets,
                plan_grid.start.saturating_sub(range.millis_i64()),
                plan_grid.end,
                true,
            )
            .await?;
        if over_budget(grid, series.len(), budget) {
            return Ok(None);
        }
        let RateRangePlan {
            ctx,
            plan,
            labels_by_fp,
        } = plan_rate_range_selector(series, plan_grid, range, kind).await?;
        let batches = ctx.execute_logical_plan(plan).await?.collect().await?;
        let steps = assemble_range_fold_grid(&batches, plan_grid, grid, RATE_VALUE_COLUMN)?;
        // Rate-family results drop the metric name, as `assemble_rate_batches`
        // does for one step.
        let labels_by_fp = labels_by_fp
            .iter()
            .map(|(fp, labels)| (*fp, labels_without_metric_name(labels)))
            .collect();
        Ok(Some(GridVectors::new(grid, labels_by_fp, steps, true)))
    }

    /// Plans and executes an `*_over_time` fold over the whole grid.
    async fn build_grid_over_time(
        &self,
        tenant: &str,
        selector: &MatrixSelector,
        grid: StepGrid,
        family: OverTimeFamily,
        phi: f64,
        budget: usize,
    ) -> Result<Option<GridVectors>> {
        let Some(plan_grid) = shifted_grid(grid, selector.vs.offset.as_ref())? else {
            return Ok(None);
        };
        let range = selector_duration(selector.range)?;
        let matcher_sets = label_matcher_sets(&selector.vs);
        let series = self
            .labeled_series_sets(
                tenant,
                &matcher_sets,
                plan_grid.start.saturating_sub(range.millis_i64()),
                plan_grid.end,
                true,
            )
            .await?;
        if over_budget(grid, series.len(), budget) {
            return Ok(None);
        }
        let OverTimeRangePlan {
            ctx,
            plan,
            labels_by_fp,
        } = plan_over_time_range_selector(series, plan_grid, range, family, phi).await?;
        let batches = ctx.execute_logical_plan(plan).await?.collect().await?;
        let steps = assemble_range_fold_grid(&batches, plan_grid, grid, OVER_TIME_VALUE_COLUMN)?;
        // Only `last_over_time` preserves the metric name, as
        // `assemble_over_time_batches` decides for one step.
        let preserve_metric_name = matches!(family, OverTimeFamily::Last);
        let labels_by_fp: BTreeMap<SeriesFingerprint, Labels> = labels_by_fp
            .iter()
            .map(|(fp, labels)| {
                let labels = if preserve_metric_name {
                    labels.clone()
                } else {
                    labels_without_metric_name(labels)
                };
                (*fp, labels)
            })
            .collect();
        Ok(Some(GridVectors::new(
            grid,
            labels_by_fp,
            steps,
            !preserve_metric_name,
        )))
    }
}

/// The active range query's leaf memo and grid, when `time_ms` is on that grid.
///
/// An instant query has no memo at all, and a subquery's sub-step lands between
/// the outer grid's instants. Both leave the caller on the per-step path.
fn active_step_grid(time_ms: i64) -> Option<(StepVectorCache, StepGrid)> {
    let (cache, grid) = RANGE_STEP_VECTORS
        .try_with(|cache| {
            let grid = cache.lock().expect("range step vector cache poisoned").grid;
            (Arc::clone(cache), grid)
        })
        .ok()?;
    grid.index_of(time_ms)?;
    Some((cache, grid))
}

/// A leaf with no scalar parameter to distinguish one memo from another.
const NO_PARAMETER: u64 = 0;

/// What the memo can do for `key` at `parameter` on this step.
///
/// A leaf built at a different parameter cannot answer this step. The parameter
/// is resolved per step, so a leaf whose parameter moves would otherwise be
/// planned over the whole grid again at every step — worse than never having
/// hoisted it. The memo gives such a leaf up instead: this step and every later
/// one take the per-step path.
fn leaf_lookup(cache: &StepVectorCache, key: &str, parameter: u64) -> LeafLookup {
    let mut guard = cache.lock().expect("range step vector cache poisoned");
    match guard.leaves.get(key) {
        None => LeafLookup::Build,
        Some(LeafMemo::Declined) => LeafLookup::Declined,
        Some(LeafMemo::Ready {
            parameter: built_with,
            vectors,
        }) => {
            if *built_with == parameter {
                return LeafLookup::Ready(Arc::clone(vectors));
            }
            guard.leaves.insert(key.to_string(), LeafMemo::Declined);
            LeafLookup::Declined
        }
    }
}

/// Points the memo may still take.
fn remaining_budget(cache: &StepVectorCache) -> usize {
    let used = cache
        .lock()
        .expect("range step vector cache poisoned")
        .points;
    MAX_GRID_LEAF_POINTS.saturating_sub(used)
}

/// Records a built leaf, and hands it back for this step's lookup.
///
/// A leaf already given up stays given up: a build that raced it, or one whose
/// parameter differs from the recorded one, must not put it back.
fn store_leaf(
    cache: &StepVectorCache,
    key: String,
    parameter: u64,
    built: Option<GridVectors>,
) -> Option<Arc<GridVectors>> {
    let mut guard = cache.lock().expect("range step vector cache poisoned");
    if matches!(guard.leaves.get(&key), Some(LeafMemo::Declined)) {
        return None;
    }
    let Some(vectors) = built else {
        guard.leaves.insert(key, LeafMemo::Declined);
        return None;
    };
    let vectors = Arc::new(vectors);
    guard.points = guard.points.saturating_add(vectors.point_count());
    guard.leaves.insert(
        key,
        LeafMemo::Ready {
            parameter,
            vectors: Arc::clone(&vectors),
        },
    );
    Some(vectors)
}

/// The grid the plan runs on: the query's grid with the selector's `offset`
/// folded in.
///
/// Every instant shifts by the same constant, so the plan's grid keeps the
/// query's stride and point count and a position on one is the same position on
/// the other. Returns `None` if that fails to hold — an offset large enough to
/// saturate one end of the grid but not the other — leaving the caller on the
/// per-step path, which resolves each instant's modifier itself.
fn shifted_grid(grid: StepGrid, offset: Option<&Offset>) -> Result<Option<StepGrid>> {
    let shifted = StepGrid {
        start: apply_selector_time_modifier(grid.start, None, offset, None)?,
        end: apply_selector_time_modifier(grid.end, None, offset, None)?,
        step: grid.step,
    };
    if shifted.end.checked_sub(shifted.start) != grid.end.checked_sub(grid.start) {
        return Ok(None);
    }
    Ok(Some(shifted))
}

/// Whether a leaf over `series` series would take more than `budget` points.
fn over_budget(grid: StepGrid, series: usize, budget: usize) -> bool {
    grid.point_count()
        .checked_mul(series)
        .is_none_or(|points| points > budget)
}
