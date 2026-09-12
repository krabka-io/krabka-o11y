use super::{
    AnnotatedQueryResult, FrontendRangeRequest, PromqlError, QueryShardExecution,
    QueryShardReducer, RangeQueryCache, RangeQueryExecutor, TimeExt,
    execute_avg_range_query_frontend, execute_moment_range_query_frontend,
    execute_planned_range_queries, merge_range_query_results_with_reducer, plan_range_query,
    query_shard_execution, reduce_rank_range_query_results,
};

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
        Some((*k, *kind, modifier.clone()))
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
    let (results, annotations) =
        execute_planned_range_queries(executor, cache, &request.tenant, planned).await?;

    let QueryShardExecution::Merge(reducer) = execution else {
        if let Some((k, kind, modifier)) = rank {
            let merged = merge_range_query_results_with_reducer(results, QueryShardReducer::First)?;
            return Ok(AnnotatedQueryResult {
                result: reduce_rank_range_query_results(merged, k, kind, modifier.as_ref())?,
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
