use super::{
    AnnotatedQueryResult, FrontendRangeQuery, PromqlError, RangeQueryCache, RangeQueryExecutor,
    TenantId,
};

pub(crate) async fn execute_single_range_query<E, C>(
    executor: &E,
    cache: &C,
    tenant: &TenantId,
    subquery: &FrontendRangeQuery,
) -> Result<AnnotatedQueryResult, PromqlError>
where
    E: RangeQueryExecutor,
    C: RangeQueryCache + ?Sized,
{
    if let Some(cached) = cache.get(tenant.as_str(), subquery).await? {
        return Ok(cached);
    }
    let annotated = executor.execute_range_query(tenant, subquery).await?;
    cache
        .insert(tenant.as_str(), subquery, annotated.clone())
        .await?;
    Ok(annotated)
}
