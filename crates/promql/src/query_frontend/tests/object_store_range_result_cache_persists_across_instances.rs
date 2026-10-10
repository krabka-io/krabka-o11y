use super::*;

#[tokio::test]
pub(crate) async fn object_store_range_result_cache_persists_across_instances() {
    let object_store = std::sync::Arc::new(object_store::memory::InMemory::new());
    let first = ObjectStoreQueryFrontendCache::new(object_store.clone(), "query-cache".to_string());
    let second = ObjectStoreQueryFrontendCache::new(object_store, "query-cache".to_string());
    let query = up_range_query(0, Some(QueryShard { index: 1, total: 2 }));
    let result = one_sample_matrix(labels(&[("__name__", "up"), ("job", "api")]));

    first
        .insert("tenant-a", &query, result.clone())
        .await
        .unwrap();

    assert2::assert!(second.get("tenant-a", &query).await.unwrap() == Some(result));
}
