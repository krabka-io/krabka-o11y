use super::{
    FrontendRangeQuery, PromqlError, QueryResult, RangeQueryCache, RangeQueryExecutor,
    execute_single_range_query,
};

/// Executes the planned sub-queries concurrently, one per sub-range and shard.
///
/// The planned sub-queries are independent, so this function dispatches them all
/// at once with [`futures::future::join_all`] and does not await them one by
/// one. It collects the results by planned position, so the order does not
/// depend on which sub-query completes first. The matrix-stitching merge needs
/// that deterministic order. The [`RangeQueryExecutor`] and [`RangeQueryCache`]
/// bounds are `Send + Sync`, so the per-sub-query futures are `Send` and safe to
/// drive together.
pub(crate) async fn execute_planned_range_queries<E, C>(
    executor: &E,
    cache: &C,
    tenant: &str,
    planned: Vec<FrontendRangeQuery>,
) -> Result<Vec<QueryResult>, PromqlError>
where
    E: RangeQueryExecutor,
    C: RangeQueryCache + ?Sized,
{
    let futures = planned
        .iter()
        .map(|subquery| execute_single_range_query(executor, cache, tenant, subquery));
    futures::future::join_all(futures)
        .await
        .into_iter()
        .collect()
}
