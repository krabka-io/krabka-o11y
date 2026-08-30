use super::*;

#[tokio::test]
pub(crate) async fn object_store_range_result_cache_expires_stale_objects() {
    let object_store = std::sync::Arc::new(object_store::memory::InMemory::new());
    let clock = Arc::new(ManualClock::new(5_000_000));
    let cache = ObjectStoreQueryFrontendCache::new(object_store, "query-cache".to_string())
        .with_ttl(secs(30))
        .with_clock(clock.clone());
    let query = FrontendRangeQuery {
        query: "up".into(),
        start_ms: 0,
        end_ms: 0,
        step: millis(60_000),
        shard: None,
    };
    let result = QueryResult::RangeMatrix(vec![RangeSeries {
        labels: labels(&[("__name__", "up")]),
        samples: vec![(0, SampleValue::Float(1.0))],
    }]);

    cache
        .insert("tenant-a", &query, result.clone())
        .await
        .unwrap();

    // Within TTL: hit.
    clock.advance(29_000);
    assert2::assert!(cache.get("tenant-a", &query).await.unwrap() == Some(result));

    // Past TTL: miss.
    clock.advance(2_000);
    assert2::assert!(cache.get("tenant-a", &query).await.unwrap() == None);
}
