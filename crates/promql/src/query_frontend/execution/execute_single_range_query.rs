use super::{
    FrontendRangeQuery, PromqlError, QueryResult, RangeQueryCache, RangeQueryExecutor, TenantId,
};

pub(crate) async fn execute_single_range_query<E, C>(
    executor: &E,
    cache: &C,
    tenant: &TenantId,
    subquery: &FrontendRangeQuery,
) -> Result<QueryResult, PromqlError>
where
    E: RangeQueryExecutor,
    C: RangeQueryCache + ?Sized,
{
    if let Some(result) = cache.get(tenant.as_str(), subquery).await? {
        return Ok(result);
    }
    let result = executor.execute_range_query(tenant, subquery).await?;
    cache
        .insert(tenant.as_str(), subquery, result.clone())
        .await?;
    Ok(result)
}
