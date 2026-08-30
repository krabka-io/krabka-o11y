use super::*;

pub(crate) async fn execute_moment_range_query_frontend<E, C>(
    executor: &E,
    cache: &C,
    request: &FrontendRangeRequest,
    sum_query: &str,
    count_query: &str,
    sum_squares_query: &str,
    kind: MomentReduction,
) -> Result<QueryResult, PromqlError>
where
    E: RangeQueryExecutor,
    C: RangeQueryCache + ?Sized,
{
    let sum_plan = plan_range_query(
        sum_query,
        request.start_ms,
        request.end_ms,
        request.step,
        request.opts,
    )?;
    let count_plan = plan_range_query(
        count_query,
        request.start_ms,
        request.end_ms,
        request.step,
        request.opts,
    )?;
    let sum_squares_plan = plan_range_query(
        sum_squares_query,
        request.start_ms,
        request.end_ms,
        request.step,
        request.opts,
    )?;
    let sum_results =
        execute_planned_range_queries(executor, cache, &request.tenant, sum_plan).await?;
    let count_results =
        execute_planned_range_queries(executor, cache, &request.tenant, count_plan).await?;
    let sum_squares_results =
        execute_planned_range_queries(executor, cache, &request.tenant, sum_squares_plan).await?;
    let sums = merge_range_query_results_with_reducer(sum_results, QueryShardReducer::Sum)?;
    let counts = merge_range_query_results_with_reducer(count_results, QueryShardReducer::Sum)?;
    let sum_squares =
        merge_range_query_results_with_reducer(sum_squares_results, QueryShardReducer::Sum)?;
    reduce_moment_range_query_results(sums, counts, sum_squares, kind)
}
