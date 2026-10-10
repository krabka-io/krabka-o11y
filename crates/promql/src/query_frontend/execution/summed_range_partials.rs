use super::{
    Annotations, FrontendRangeRequest, PromqlError, QueryResult, QueryShardReducer,
    RangeQueryCache, RangeQueryExecutor, execute_planned_range_queries,
    merge_range_query_results_with_reducer, plan_range_query,
};

/// One frontend range request, and the executor and cache that answer its
/// partial queries.
pub(crate) struct FrontendRangeRun<'a, E: ?Sized, C: ?Sized> {
    pub(crate) executor: &'a E,
    pub(crate) cache: &'a C,
    pub(crate) request: &'a FrontendRangeRequest,
}

impl<E, C> FrontendRangeRun<'_, E, C>
where
    E: RangeQueryExecutor,
    C: RangeQueryCache + ?Sized,
{
    /// Plans every one of `queries` over the request's range, then executes
    /// them in order, then sums each one's shards.
    ///
    /// Returns each query's summed result, in the order of `queries`, and the
    /// annotations of all of them.
    pub(crate) async fn summed_partials<const N: usize>(
        &self,
        queries: [&str; N],
    ) -> Result<([QueryResult; N], Annotations), PromqlError> {
        let request = self.request;
        let plans = queries
            .into_iter()
            .map(|query| {
                plan_range_query(
                    query,
                    request.start_ms,
                    request.end_ms,
                    request.step,
                    request.opts,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut executed = Vec::with_capacity(N);
        for plan in plans {
            executed.push(
                execute_planned_range_queries(
                    self.executor,
                    self.cache,
                    &request.tenant,
                    plan,
                    request.admission_limits,
                )
                .await?,
            );
        }
        let mut annotations: Option<Annotations> = None;
        let mut summed = Vec::with_capacity(N);
        for (results, partial_annotations) in executed {
            match &mut annotations {
                Some(annotations) => annotations.extend(&partial_annotations),
                None => annotations = Some(partial_annotations),
            }
            summed.push(merge_range_query_results_with_reducer(
                results,
                QueryShardReducer::Sum,
            )?);
        }
        let summed = <[QueryResult; N]>::try_from(summed)
            .unwrap_or_else(|_| unreachable!("one summed result per query"));
        Ok((summed, annotations.unwrap_or_default()))
    }
}
