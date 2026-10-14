use super::*;

#[tokio::test]
pub(crate) async fn object_store_range_result_cache_expires_stale_objects() {
    let object_store = std::sync::Arc::new(object_store::memory::InMemory::new());
    let clock = Arc::new(ManualClock::new(5_000_000));
    let cache = ObjectStoreQueryFrontendCache::new(object_store, "query-cache".to_string())
        .with_ttl(secs(30))
        .with_clock(clock.clone());
    let fixture = up_query_and_result();
    let query = fixture.query;
    let result = fixture.result;

    assert_ttl_hit_then_miss(&cache, &clock, &query, &result, 29_000, 2_000).await;

    cache
        .insert("tenant-a", &query, result)
        .await
        .expect("cache insert");
    clock.advance(31_000);
    assert2::assert!(
        krabka_query_frontend::QueryCache::sweep(&cache)
            .await
            .unwrap()
            == 1
    );
}
