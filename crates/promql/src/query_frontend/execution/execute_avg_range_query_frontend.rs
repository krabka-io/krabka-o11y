use super::{FrontendRangeRequest, PromqlError, QueryResult, QueryShardReducer, RangeQueryCache, RangeQueryExecutor, divide_range_query_results, execute_planned_range_queries, merge_range_query_results_with_reducer, plan_range_query};

pub(crate) async fn execute_avg_range_query_frontend<E, C>(
    executor: &E,
    cache: &C,
    request: &FrontendRangeRequest,
    sum_query: &str,
    count_query: &str,
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
    let sum_results =
        execute_planned_range_queries(executor, cache, &request.tenant, sum_plan).await?;
    let count_results =
        execute_planned_range_queries(executor, cache, &request.tenant, count_plan).await?;
    let sums = merge_range_query_results_with_reducer(sum_results, QueryShardReducer::Sum)?;
    let counts = merge_range_query_results_with_reducer(count_results, QueryShardReducer::Sum)?;
    divide_range_query_results(sums, counts)
}
