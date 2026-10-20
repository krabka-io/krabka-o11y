//! Bounded union grids for overlapping subquery windows.

use std::{cell::RefCell, sync::Arc};

use krabka_units::prelude::*;
use promql_parser::parser::{Expr, SubqueryExpr};

use super::{
    PromqlEngine, annotations::ANNOTATIONS, apply_selector_time_modifier, check_resolution_points,
    grid_leaf::MAX_GRID_LEAF_POINTS, query_stats_enabled, range_functions::align_subquery_start,
    selector_duration, step_vectors::RANGE_STEP_VECTORS,
};
use crate::{
    Annotations, error::Result, planner::StepGrid, result::RangeSeries, store::MetricStore,
};

#[cfg(test)]
tokio::task_local! {
    pub(super) static SUBQUERY_GRID_DISABLED: ();
}

pub(crate) struct SubqueryGrid {
    series: Vec<RangeSeries>,
}

impl SubqueryGrid {
    fn window(&self, start_ms: i64, end_ms: i64) -> Vec<RangeSeries> {
        self.series
            .iter()
            .filter_map(|series| {
                let start = series
                    .samples
                    .partition_point(|s| s.timestamp_ms < start_ms);
                let end = series.samples.partition_point(|s| s.timestamp_ms <= end_ms);
                (start < end).then(|| RangeSeries {
                    labels: series.labels.clone(),
                    drop_name: series.drop_name,
                    start_timestamps_ms: series
                        .start_timestamps_ms
                        .range(start_ms..=end_ms)
                        .map(|entry| (*entry.0, *entry.1))
                        .collect(),
                    samples: series.samples[start..end].to_vec(),
                })
            })
            .collect()
    }
}

impl<S: MetricStore> PromqlEngine<S> {
    pub(super) async fn subquery_grid_window(
        &self,
        tenant: &str,
        subquery: &SubqueryExpr,
        time_ms: i64,
    ) -> Result<Option<Vec<RangeSeries>>> {
        #[cfg(test)]
        if SUBQUERY_GRID_DISABLED.try_with(|()| ()).is_ok() {
            return Ok(None);
        }
        // Sample statistics describe each logical evaluation, including overlap.
        if query_stats_enabled() {
            return Ok(None);
        }
        let Ok(cache) = RANGE_STEP_VECTORS.try_with(Arc::clone) else {
            return Ok(None);
        };
        let outer = cache.lock().expect("range step vector cache poisoned").grid;
        if outer.index_of(time_ms).is_none() || outer.point_count() <= 1 {
            return Ok(None);
        }
        let range = selector_duration(subquery.range)?;
        let step = subquery
            .step
            .map(selector_duration)
            .transpose()?
            .unwrap_or(self.opts.eval_interval);
        let end_ms = apply_selector_time_modifier(
            time_ms,
            subquery.at.as_ref(),
            subquery.offset.as_ref(),
            None,
        )?;
        let start_ms = align_subquery_start(end_ms.saturating_sub(range.millis_i64()), step);
        check_resolution_points(start_ms, end_ms, step)?;
        // The parent cache fixes the tenant, engine options, @ bounds, and
        // experimental query context. The full subquery text fixes the remaining
        // inputs, including its resolution, offset, and @ modifier.
        let key = subquery.to_string();
        let entry = cache
            .lock()
            .expect("range step vector cache poisoned")
            .subqueries
            .get(&key)
            .cloned();
        if let Some(entry) = entry {
            return Ok(entry.as_ref().map(|grid| grid.window(start_ms, end_ms)));
        }
        let union = subquery_union_grid(subquery, outer, range, step)?;
        let budget = MAX_GRID_LEAF_POINTS.saturating_sub(
            cache
                .lock()
                .expect("range step vector cache poisoned")
                .points,
        );
        let built = if let Some(union) = union.filter(|_| budget > 0) {
            self.build_subquery_union(
                tenant,
                &subquery.expr,
                union,
                StepGrid {
                    start: start_ms,
                    end: end_ms,
                    step: step.millis_i64(),
                },
                budget,
            )
            .await
        } else {
            None
        };
        let built = built.map(|series| Arc::new(SubqueryGrid { series }));
        let answer = built.as_ref().map(|grid| grid.window(start_ms, end_ms));
        let points = built.as_ref().map_or(0, |grid| {
            grid.series.iter().map(|s| s.samples.len()).sum::<usize>()
        });
        let mut guard = cache.lock().expect("range step vector cache poisoned");
        guard.points += points;
        guard.subqueries.insert(key, built);
        Ok(answer)
    }

    async fn build_subquery_union(
        &self,
        tenant: &str,
        expr: &Expr,
        union: StepGrid,
        first_window: StepGrid,
        budget: usize,
    ) -> Option<Vec<RangeSeries>> {
        // A union can exceed a limit that each window obeys. Keep speculative
        // errors and annotations out of the caller's evaluation.
        let (series, annotations) = ANNOTATIONS
            .scope(RefCell::new(Annotations::new()), async {
                let result = self
                    .eval_range_via_planner_bounded(tenant, expr, union, budget)
                    .await;
                (
                    result.ok().flatten(),
                    ANNOTATIONS.with(|sink| sink.borrow().clone()),
                )
            })
            .await;
        let series = series?;
        if !annotations.is_empty() {
            // Plain annotations are deduplicated. Reuse them only when the
            // first window already emits the union's complete set in the same
            // order. Structured repairs accumulate counts and cannot be reused.
            if !annotations.histogram_quantile_repairs.is_empty() {
                return None;
            }
            let first_annotations = ANNOTATIONS
                .scope(RefCell::new(Annotations::new()), async {
                    self.eval_range_via_planner_bounded(tenant, expr, first_window, usize::MAX)
                        .await
                        .ok()
                        .flatten()?;
                    Some(ANNOTATIONS.with(|sink| sink.borrow().clone()))
                })
                .await?;
            if first_annotations != annotations {
                return None;
            }
            let _ = ANNOTATIONS.try_with(|sink| sink.borrow_mut().extend(&annotations));
        }
        Some(series)
    }
}

fn subquery_union_grid(
    subquery: &SubqueryExpr,
    outer: StepGrid,
    range: Time,
    step: Time,
) -> Result<Option<StepGrid>> {
    // Disjoint windows would evaluate unused instants and could add annotations.
    if outer.step > range.millis_i64() && subquery.at.is_none() {
        return Ok(None);
    }
    let first_end = apply_selector_time_modifier(
        outer.start,
        subquery.at.as_ref(),
        subquery.offset.as_ref(),
        None,
    )?;
    let last_outer = outer.start.saturating_add(
        i64::try_from(outer.point_count().saturating_sub(1))
            .unwrap_or(i64::MAX)
            .saturating_mul(outer.step),
    );
    let end = apply_selector_time_modifier(
        last_outer,
        subquery.at.as_ref(),
        subquery.offset.as_ref(),
        None,
    )?;
    let start = align_subquery_start(first_end.saturating_sub(range.millis_i64()), step);
    if check_resolution_points(start, end, step).is_err() {
        return Ok(None);
    }
    Ok(Some(StepGrid {
        start,
        end,
        step: step.millis_i64(),
    }))
}
