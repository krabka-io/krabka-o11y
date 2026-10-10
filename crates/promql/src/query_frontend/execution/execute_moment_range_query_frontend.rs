use super::{
    AnnotatedQueryResult, FrontendRangeRequest, FrontendRangeRun, MomentReduction, PromqlError,
    RangeQueryCache, RangeQueryExecutor, reduce_moment_range_query_results,
};

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
    let ([sums, counts, sum_squares], annotations) = run
        .summed_partials([sum_query, count_query, sum_squares_query])
        .await?;
    Ok(AnnotatedQueryResult {
        result: reduce_moment_range_query_results(sums, counts, sum_squares, kind)?,
        annotations,
    })
}
