use super::*;

#[tokio::test]
pub(crate) async fn object_store_range_result_cache_persists_across_instances() {
    let object_store = std::sync::Arc::new(object_store::memory::InMemory::new());
    let first = ObjectStoreQueryFrontendCache::new(object_store.clone(), "query-cache".to_string());
    let second = ObjectStoreQueryFrontendCache::new(object_store, "query-cache".to_string());
    let query = FrontendRangeQuery {
        query: "up".into(),
        start_ms: 0,
        end_ms: 0,
        step: millis(60_000),
        shard: Some(QueryShard { index: 1, total: 2 }),
    };
    let result = QueryResult::RangeMatrix(vec![RangeSeries {
        labels: labels(&[("__name__", "up"), ("job", "api")]),
        samples: vec![(0, SampleValue::Float(1.0))],
    }]);

    first
        .insert("tenant-a", &query, result.clone())
        .await
        .unwrap();

    assert2::assert!(second.get("tenant-a", &query).await.unwrap() == Some(result));
}
