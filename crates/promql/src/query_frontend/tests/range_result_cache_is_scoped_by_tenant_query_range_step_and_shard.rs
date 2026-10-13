use super::{cached_up_matrix::one_up_api_sample_matrix, *};

#[tokio::test]
pub(crate) async fn range_result_cache_is_scoped_by_tenant_query_range_step_and_shard() {
    let cache = QueryFrontendCache::default();
    let query = up_range_query(60_000, Some(QueryShard { index: 1, total: 2 }));
    let result = one_up_api_sample_matrix();

    cache
        .insert("tenant-a", &query, result.clone())
        .await
        .unwrap();

    assert2::assert!(cache.get("tenant-a", &query).await.unwrap() == Some(result));
    assert2::assert!(cache.get("tenant-b", &query).await.unwrap() == None);

    let other_shard = FrontendRangeQuery {
        shard: Some(QueryShard { index: 2, total: 2 }),
        ..query
    };
    assert2::assert!(cache.get("tenant-a", &other_shard).await.unwrap() == None);
}
