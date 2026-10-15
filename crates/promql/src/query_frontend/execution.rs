use promql_parser::parser::LabelModifier;
use async_trait::async_trait;
use krabka_blockstore::TenantId;
use krabka_units::prelude::*;

use super::{
    FrontendRangeQuery, FrontendRangeRequest, MomentReduction, RankReduction, QueryShardExecution,
    QueryShardReducer,
    cache::{RangeQueryCache, range_cache_key},
    merge::{
        divide_range_query_results, merge_range_query_results_with_reducer,
        reduce_moment_range_query_results, reduce_rank_range_query_results,
    },
    plan::{plan_range_query, query_shard_execution, query_with_shard_selector},
};
use crate::{
    AnnotatedQueryResult, Annotations, MetricStore, PromqlEngine, PromqlError, QueryResult,
};
mod execute_planned_range_queries;
mod range_query_executor;
mod summed_range_partials;
pub(super) use execute_planned_range_queries::execute_planned_range_queries;
pub use range_query_executor::RangeQueryExecutor;
use summed_range_partials::FrontendRangeRun;

pub(crate) async fn execute_avg_range_query_frontend<E, C>(
    executor: &E,
    cache: &C,
    request: &FrontendRangeRequest,
    sum_query: &str,
    count_query: &str,
) -> Result<AnnotatedQueryResult, PromqlError>
where
    E: RangeQueryExecutor,
    C: RangeQueryCache + ?Sized,
{
    let run = FrontendRangeRun {
        executor,
        cache,
        request,
    };
    let partials = run.summed_partials([sum_query, count_query]).await?;
    let [sums, counts] = partials.results;
    let annotations = partials.annotations;
    Ok(AnnotatedQueryResult {
        result: divide_range_query_results(sums, counts)?,
        annotations,
    })
}

#[async_trait]
impl<S: MetricStore> RangeQueryExecutor for PromqlEngine<S> {
    async fn execute_range_query(
        &self,
        tenant: &TenantId,
        query: &FrontendRangeQuery,
    ) -> Result<AnnotatedQueryResult, PromqlError> {
        let query_text = match query.shard {
            Some(shard) => query_with_shard_selector(&query.query, shard)?,
            None => query.query.clone(),
        };

        self.query_range_with_annotations(
            tenant,
            &query_text,
            query.start_ms,
            query.end_ms,
            query.step,
        )
        .await
    }
}


pub(crate) async fn execute_moment_range_query_frontend<E, C>(
    executor: &E,
    cache: &C,
    request: &FrontendRangeRequest,
    sum_query: &str,
    count_query: &str,
    sum_squares_query: &str,
    kind: MomentReduction,
) -> Result<AnnotatedQueryResult, PromqlError>
where
    E: RangeQueryExecutor,
    C: RangeQueryCache + ?Sized,
{
    let run = FrontendRangeRun {
        executor,
        cache,
        request,
    };
    let partials = run
        .summed_partials([sum_query, count_query, sum_squares_query])
        .await?;
    let [sums, counts, sum_squares] = partials.results;
    let annotations = partials.annotations;
    Ok(AnnotatedQueryResult {
        result: reduce_moment_range_query_results(sums, counts, sum_squares, kind)?,
        annotations,
    })
}

/// Executes a range query through query-frontend planning, cache, and merge.
///
/// The returned annotations are the merged annotations of every sub-query the
/// plan produced, cached ones included. A query therefore reports the same
/// warnings and infos whether the frontend splits it, shards it, or answers it
/// from the cache.
#[tracing::instrument(
    name = "promql.query_frontend_range",
    level = "info",
    skip_all,
    fields(
        tenant = %request.tenant,
        query = %request.query,
        start_ms = request.start_ms,
        end_ms = request.end_ms,
        step_ms = request.step.millis_i64()
    ),
    err
)]
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn execute_range_query_frontend<E, C>(
    executor: &E,
    cache: &C,
    request: &FrontendRangeRequest,
) -> Result<AnnotatedQueryResult, PromqlError>
where
    E: RangeQueryExecutor,
    C: RangeQueryCache + ?Sized,
{
    let execution = query_shard_execution(&request.query)?;
    if let QueryShardExecution::Avg {
        sum_query,
        count_query,
    } = execution
    {
        return execute_avg_range_query_frontend(
            executor,
            cache,
            request,
            &sum_query,
            &count_query,
        )
        .await;
    }
    if let QueryShardExecution::Moments {
        sum_query,
        count_query,
        sum_squares_query,
        kind,
    } = execution
    {
        return execute_moment_range_query_frontend(
            executor,
            cache,
            request,
            &sum_query,
            &count_query,
            &sum_squares_query,
            kind,
        )
        .await;
    }
    let rank = if let QueryShardExecution::Rank { k, kind, modifier } = &execution {
        Some(RankExecution {
            k: *k,
            kind: *kind,
            modifier: modifier.clone(),
        })
    } else {
        None
    };

    let planned = plan_range_query(
        &request.query,
        request.start_ms,
        request.end_ms,
        request.step,
        request.opts,
    )?;
    let results_with_annotations = execute_planned_range_queries(
        executor,
        cache,
        &request.tenant,
        planned,
        request.admission_limits,
    )
    .await?;
    let results = results_with_annotations.results;
    let annotations = results_with_annotations.annotations;

    let QueryShardExecution::Merge(reducer) = execution else {
        if let Some(rank) = rank {
            let merged = merge_range_query_results_with_reducer(results, QueryShardReducer::First)?;
            return Ok(AnnotatedQueryResult {
                result: reduce_rank_range_query_results(
                    merged,
                    rank.k,
                    rank.kind,
                    rank.modifier.as_ref(),
                )?,
                annotations,
            });
        }
        unreachable!("partial query execution returned early")
    };
    Ok(AnnotatedQueryResult {
        result: merge_range_query_results_with_reducer(results, reducer)?,
        annotations,
    })
}

struct RankExecution {
    k: usize,
    kind: RankReduction,
    modifier: Option<LabelModifier>,
}
