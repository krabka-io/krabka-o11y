use super::{
    AnnotatedQueryResult, FrontendRangeRequest, FrontendRangeRun, PromqlError, RangeQueryCache,
    RangeQueryExecutor, divide_range_query_results,
};

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
