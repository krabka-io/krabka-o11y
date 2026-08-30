use super::*;

pub(crate) async fn execute_single_range_query<E, C>(
    executor: &E,
    cache: &C,
    tenant: &str,
    subquery: &FrontendRangeQuery,
) -> Result<QueryResult, PromqlError>
where
    E: RangeQueryExecutor,
    C: RangeQueryCache + ?Sized,
{
    if let Some(result) = cache.get(tenant, subquery).await? {
        return Ok(result);
    }
    let result = executor.execute_range_query(tenant, subquery).await?;
    cache.insert(tenant, subquery, result.clone()).await?;
    Ok(result)
}
