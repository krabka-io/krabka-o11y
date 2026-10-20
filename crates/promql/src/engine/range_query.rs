use std::{cell::RefCell, collections::BTreeMap};

use krabka_blockstore::{SeriesFingerprint, TenantId};
use krabka_units::prelude::*;
use promql_parser::parser::Expr;

use super::{
    AT_MODIFIER_BOUNDS, AtModifierBounds, PromqlEngine,
    annotations::{ANNOTATION_SOURCE, ANNOTATIONS, emit_range_sort_warnings},
    check_resolution_points,
    planner_support::range_expr_routes_through_planner,
    query_stats_step,
    result_utils::{finalize_metric_names, validate_unique_instant_labelsets},
    row_cache::{RANGE_SCAN_CACHE, RangeScanCache, RangeScanCacheInner, selector_windows},
    step_vectors::{RANGE_STEP_VECTORS, StepVectorCache, StepVectorCacheInner},
};
#[cfg(feature = "experimental-functions")]
use super::{QUERY_RANGE_CONTEXT, QueryRangeContext};
use crate::{
    Annotations, DurationExprContext, PromqlError, PromqlLabels as Labels,
    error::Result,
    parse_promql_with_duration_context,
    planner::StepGrid,
    result::{AnnotatedQueryResult, QueryResult, RangeSeries, RangeSeriesSample, SampleValue},
    store::MetricStore,
};

impl<S: MetricStore> PromqlEngine<S> {
    /// Evaluates a range query over `[start_ms, end_ms]`.
    ///
    /// # Errors
    ///
    /// Returns parse, store, execution, or unsupported-expression errors.
    pub async fn query_range(
        &self,
        tenant: &TenantId,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step: Time,
    ) -> Result<QueryResult> {
        self.query_range_with_annotations(tenant, query, start_ms, end_ms, step)
            .await
            .map(|annotated| annotated.result)
    }

    /// Evaluates a range query over `[start_ms, end_ms]` with its annotations.
    ///
    /// The annotations are the warnings and infos raised during the evaluation.
    /// This method returns them with the result.
    ///
    /// # Errors
    ///
    /// Returns parse, store, execution, or unsupported-expression errors.
    #[tracing::instrument(
        name = "promql.query_range",
        level = "info",
        skip_all,
        fields(
            tenant = %tenant,
            query = %query,
            start_ms = start_ms,
            end_ms = end_ms,
            step_ms = step.millis_i64()
        ),
        err
    )]
    pub async fn query_range_with_annotations(
        &self,
        tenant: &TenantId,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step: Time,
    ) -> Result<AnnotatedQueryResult> {
        ANNOTATION_SOURCE
            .scope(
                query.to_owned(),
                ANNOTATIONS.scope(RefCell::new(Annotations::new()), async move {
                    let mut result = self
                        .eval_range_query(tenant.as_str(), query, start_ms, end_ms, step)
                        .await?;
                    finalize_metric_names(&mut result)?;
                    validate_unique_instant_labelsets(&result)?;
                    let annotations = ANNOTATIONS.with(|sink| sink.borrow().clone());
                    Ok(AnnotatedQueryResult {
                        result,
                        annotations,
                    })
                }),
            )
            .await
    }

    #[tracing::instrument(
        name = "promql.eval_range",
        level = "debug",
        skip_all,
        fields(start_ms = start_ms, end_ms = end_ms, step_ms = step.millis_i64(), route = tracing::field::Empty),
        err
    )]
    async fn eval_range_query(
        &self,
        tenant: &str,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step: Time,
    ) -> Result<QueryResult> {
        if step <= Time::ZERO {
            return Err(PromqlError::Plan("step must be positive".to_string()));
        }
        if end_ms < start_ms {
            return Err(PromqlError::Plan("end must be >= start".to_string()));
        }

        let parsed_expr = parse_range_expression(query, start_ms, end_ms, step)?;
        let expr = unparenthesized_expr(&parsed_expr);

        // A plannable instant expression (a bare selector, a scalar expression,
        // an aggregation/binary/call/unary over those, …) routes through the
        // per-step operator planner. The planner is **total** over the step
        // grid, so `eval_range_via_planner_scoped` always returns `Ok(Some(..))`
        // for a plannable shape; an `Ok(None)` would be a planner bug, surfaced
        // as an internal error rather than silently diverging.
        if range_expr_routes_through_planner(expr) {
            tracing::Span::current().record("route", "planner");
            let Some(series) = self
                .eval_range_via_planner_scoped(tenant, expr, start_ms, end_ms, step)
                .await?
            else {
                return Err(PromqlError::Plan(
                    "planner returned no result for a plannable range query".to_string(),
                ));
            };
            return Ok(QueryResult::RangeMatrix(series));
        }

        // The only non-plannable top-level range shapes are a raw matrix
        // selector and a subquery, both of which yield a range vector directly
        // from the shared kernels (with the range query's `[start, end]`
        // bounds). Every other shape is plannable and handled above.
        match expr {
            Expr::MatrixSelector(ms) => {
                tracing::Span::current().record("route", "matrix");
                self.eval_matrix_selector(tenant, ms, start_ms, end_ms, None)
                    .await
                    .map(QueryResult::RangeMatrix)
            }
            Expr::Subquery(subquery) => {
                tracing::Span::current().record("route", "subquery");
                self.eval_subquery(tenant, subquery, end_ms)
                    .await
                    .map(QueryResult::RangeMatrix)
            }
            other => Err(PromqlError::Plan(format!(
                "planner did not claim range expression: {other}"
            ))),
        }
    }

    /// Per-step planner range driver with every configured evaluation scope.
    async fn eval_range_via_planner_scoped(
        &self,
        tenant: &str,
        expr: &Expr,
        start_ms: i64,
        end_ms: i64,
        step: Time,
    ) -> Result<Option<Vec<RangeSeries>>> {
        let future = self.eval_range_via_planner(tenant, expr, start_ms, end_ms, step);
        #[cfg(feature = "experimental-functions")]
        let future = QUERY_RANGE_CONTEXT.scope(
            QueryRangeContext {
                start_ms,
                end_ms,
                step,
            },
            future,
        );
        AT_MODIFIER_BOUNDS
            .scope(AtModifierBounds { start_ms, end_ms }, future)
            .await
    }

    /// Evaluates a plannable instant `expr` over the step grid through the planner.
    ///
    /// This method stitches the per-step instant vectors and scalars into a
    /// [`RangeMatrix`](QueryResult::RangeMatrix). It walks the step grid from
    /// `start_ms` to `end_ms` inclusive and advances by `step` with a saturating
    /// add.
    ///
    /// The method groups samples by label-set fingerprint into one series each
    /// and appends the points in step order. Gaps stay implicit: a step where a
    /// series has no value produces no point. A scalar expr folds into a single
    /// empty-label-set series. The output iterates in fingerprint order, the
    /// `BTreeMap` keys, which matches Prometheus byte-for-byte.
    ///
    /// Returns `Ok(None)` only if some step's [`Self::plan_instant_expr`] returns
    /// `None`. The planner is total, so this never happens for a plannable
    /// `expr`. The caller treats an `Ok(None)` as an internal planner bug.
    #[tracing::instrument(
        name = "promql.range_planner",
        level = "debug",
        skip_all,
        fields(start_ms = start_ms, end_ms = end_ms, step_ms = step.millis_i64(), steps = tracing::field::Empty),
        err
    )]
    pub(super) async fn eval_range_via_planner(
        &self,
        tenant: &str,
        expr: &Expr,
        start_ms: i64,
        end_ms: i64,
        step: Time,
    ) -> Result<Option<Vec<RangeSeries>>> {
        // Backstop the resolution cap before the per-step loop, so an abusive
        // subquery resolution (e.g. `last_over_time(up[1000d:1ms])`) errors
        // rather than looping ~1e11 times. The HTTP front-gate enforces the same
        // cap on the top-level query window; this guards the engine itself
        // (including subqueries, whose grid the front-gate never sees).
        let step_count = check_resolution_points(start_ms, end_ms, step)?;
        tracing::Span::current().record("steps", step_count);
        let unions = selector_windows(
            expr,
            start_ms,
            end_ms,
            self.opts.lookback_delta.millis_i64(),
        )?;
        let cache: RangeScanCache =
            std::sync::Arc::new(std::sync::Mutex::new(RangeScanCacheInner::range(
                unions,
                start_ms.saturating_sub(self.opts.lookback_delta.millis_i64()),
                end_ms,
            )));
        // Plan each operator leaf once over the whole grid rather than once per
        // step, and let the loop read the memo. Scoped like the scan cache
        // above, so a nested range evaluation (a subquery's own grid) shadows it
        // and restores this one on exit.
        let step_vectors: StepVectorCache =
            std::sync::Arc::new(std::sync::Mutex::new(StepVectorCacheInner {
                grid: StepGrid {
                    start: start_ms,
                    end: end_ms,
                    step: step.millis_i64(),
                },
                points: 0,
                leaves: std::collections::HashMap::new(),
            }));
        let driver = async move {
            let mut by_fp: BTreeMap<SeriesFingerprint, RangeSeries> = BTreeMap::new();
            let mut step_time_ms = start_ms;
            while step_time_ms <= end_ms {
                let result = query_stats_step(step_time_ms, async {
                    let Some(planned) = self.plan_instant_expr(tenant, expr, step_time_ms).await?
                    else {
                        return Ok::<Option<QueryResult>, PromqlError>(None);
                    };
                    let result = self.assemble_planned_instant(planned, step_time_ms).await?;
                    // A subquery consumes this matrix before the public query
                    // boundary. Keep its origin name and pending-removal flag
                    // for range functions and their annotations.
                    validate_unique_instant_labelsets(&result)?;
                    Ok(Some(result))
                })
                .await?;
                let Some(result) = result else {
                    // This step's shape is not planner-supported (e.g. a histogram
                    // series appeared in-window). Abandon the operator path for the
                    // whole query so the interpreter produces a consistent result.
                    return Ok(None);
                };
                match result {
                    QueryResult::InstantVector(samples) => {
                        for sample in samples {
                            let fp = sample.labels.fingerprint();
                            by_fp
                                .entry(fp)
                                .or_insert_with(|| RangeSeries {
                                    drop_name: sample.drop_name,
                                    start_timestamps_ms: std::collections::BTreeMap::new(),
                                    labels: sample.labels.clone(),
                                    samples: Vec::new(),
                                })
                                .samples
                                .push(RangeSeriesSample::new(step_time_ms, sample.value));
                        }
                    }
                    QueryResult::Scalar { value, .. } => {
                        let labels = Labels::new();
                        by_fp
                            .entry(labels.fingerprint())
                            .or_insert_with(|| RangeSeries {
                                drop_name: false,
                                start_timestamps_ms: std::collections::BTreeMap::new(),
                                labels,
                                samples: Vec::new(),
                            })
                            .samples
                            .push(RangeSeriesSample::new(
                                step_time_ms,
                                SampleValue::Float(value),
                            ));
                    }
                    QueryResult::Str { .. } | QueryResult::RangeMatrix(_) => {
                        // The planner only ever assembles an instant vector or a
                        // scalar; neither of these can arise. Fall back defensively.
                        return Ok(None);
                    }
                }
                step_time_ms = step_time_ms.saturating_add(step.millis_i64());
            }
            Ok(Some(by_fp.into_values().collect()))
        };
        RANGE_SCAN_CACHE
            .scope(cache, RANGE_STEP_VECTORS.scope(step_vectors, driver))
            .await
    }

    /// Forces the per-step planner range driver past the production gate.
    ///
    /// This method bypasses [`range_expr_routes_through_planner`] and is the
    /// parity-test seam. It lets the differential test drive every range case
    /// through the operator path and compare the output to the interpreter's
    /// `query_range`, which proves parity before the gate is trusted. The cases
    /// include a bare top-level selector, which the production gate keeps on the
    /// interpreter.
    #[cfg(test)]
    pub(super) async fn eval_range_via_planner_forced(
        &self,
        tenant: &str,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step: Time,
    ) -> Result<QueryResult> {
        let parsed_expr = parse_range_expression(query, start_ms, end_ms, step)?;
        let expr = unparenthesized_expr(&parsed_expr);
        let series = self
            .eval_range_via_planner_scoped(tenant, expr, start_ms, end_ms, step)
            .await?
            .expect("forced planner range driver returned None");
        let mut result = QueryResult::RangeMatrix(series);
        finalize_metric_names(&mut result)?;
        validate_unique_instant_labelsets(&result)?;
        Ok(result)
    }
}

fn parse_range_expression(query: &str, start_ms: i64, end_ms: i64, step: Time) -> Result<Expr> {
    let expr = parse_promql_with_duration_context(
        query,
        DurationExprContext::range(start_ms, end_ms, step),
    )?;
    emit_range_sort_warnings(&expr);
    Ok(expr)
}

fn unparenthesized_expr(mut expr: &Expr) -> &Expr {
    while let Expr::Paren(paren) = expr {
        expr = &paren.expr;
    }
    expr
}
