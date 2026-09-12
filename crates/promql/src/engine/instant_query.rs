use std::cell::RefCell;

use krabka_blockstore::TenantId;
use promql_parser::parser::Expr;

use super::{
    AT_MODIFIER_BOUNDS, AtModifierBounds, PromqlEngine,
    annotations::ANNOTATIONS,
    result_utils::{finalize_metric_names, validate_unique_instant_labelsets},
};
use crate::{
    DurationExprContext, PromqlError,
    error::Result,
    parse_promql_with_duration_context,
    result::{Annotations, QueryResult},
    store::MetricStore,
};

impl<S: MetricStore> PromqlEngine<S> {
    /// Evaluates an instant query at `time_ms`.
    ///
    /// # Errors
    ///
    /// Returns parse, store, execution, or unsupported-expression errors.
    pub async fn query_instant(
        &self,
        tenant: &TenantId,
        query: &str,
        time_ms: i64,
    ) -> Result<QueryResult> {
        self.query_instant_with_annotations(tenant, query, time_ms)
            .await
            .map(|(result, _)| result)
    }

    /// Evaluates an instant query at `time_ms` and returns the annotations.
    ///
    /// The annotations are the warnings and infos that the evaluation raised.
    /// This method returns them with the result.
    ///
    /// # Errors
    ///
    /// Returns parse, store, execution, or unsupported-expression errors.
    #[tracing::instrument(
        name = "promql.query_instant",
        level = "info",
        skip_all,
        fields(tenant = %tenant, query = %query, time_ms = time_ms),
        err
    )]
    pub async fn query_instant_with_annotations(
        &self,
        tenant: &TenantId,
        query: &str,
        time_ms: i64,
    ) -> Result<(QueryResult, Annotations)> {
        ANNOTATIONS
            .scope(RefCell::new(Annotations::new()), async move {
                AT_MODIFIER_BOUNDS
                    .scope(
                        AtModifierBounds {
                            start_ms: time_ms,
                            end_ms: time_ms,
                        },
                        async move {
                            let expr = parse_promql_with_duration_context(
                                query,
                                DurationExprContext::instant(time_ms),
                            )?;
                            let mut result = self
                                .eval_top_level_instant_expr(tenant.as_str(), &expr, time_ms)
                                .await?;
                            if let QueryResult::InstantVector(samples) = &mut result {
                                for sample in samples {
                                    sample.ts_ms = time_ms;
                                }
                            }
                            finalize_metric_names(&mut result);
                            validate_unique_instant_labelsets(&result)?;
                            let annotations = ANNOTATIONS.with(|sink| sink.borrow().clone());
                            Ok((result, annotations))
                        },
                    )
                    .await
            })
            .await
    }

    /// Dispatches a top-level instant query.
    ///
    /// This method passes the query to the recursive operator planner
    /// (`Self::plan_instant_expr`). The planner dispatches on the `PromQL` `Expr`
    /// node kind and assembles a `DataFusion` `LogicalPlan` over the custom
    /// operators. It also reuses the shared leaf kernels for histogram-bearing
    /// shapes and other directly-materialized shapes. The planner is total: it
    /// returns `Ok(Some(..))` for every valid query and `Err(..)` for every
    /// invalid one, and never `Ok(None)`. This method then executes the plan and
    /// assembles the output batches into the result.
    ///
    /// `Ok(None)` is therefore unreachable. If it does occur, it is a planner
    /// bug, and this method returns an internal `PromqlError::Plan`.
    #[tracing::instrument(
        name = "promql.eval_instant",
        level = "debug",
        skip_all,
        fields(time_ms = time_ms),
        err
    )]
    async fn eval_top_level_instant_expr(
        &self,
        tenant: &str,
        expr: &Expr,
        time_ms: i64,
    ) -> Result<QueryResult> {
        let Some(planned) = self.plan_instant_expr(tenant, expr, time_ms).await? else {
            return Err(PromqlError::Plan(
                "planner returned no result for a valid instant query".to_string(),
            ));
        };
        self.assemble_planned_instant(planned, time_ms).await
    }
}
