use super::*;

#[tokio::test]
pub(crate) async fn object_store_range_result_cache_expires_stale_objects() {
    let object_store = std::sync::Arc::new(object_store::memory::InMemory::new());
    let clock = Arc::new(ManualClock::new(5_000_000));
    let cache = ObjectStoreQueryFrontendCache::new(object_store, "query-cache".to_string())
        .with_ttl(secs(30))
        .with_clock(clock.clone());
    let query = up_range_query(0, None);
    let result = one_sample_matrix(labels(&[("__name__", "up")]));

    cache
        .insert("tenant-a", &query, result.clone())
        .await
        .unwrap();

    // Within TTL: hit.
    clock.advance(29_000);
    assert2::assert!(cache.get("tenant-a", &query).await.unwrap() == Some(result.clone()));

    // Past TTL: miss.
    clock.advance(2_000);
    assert2::assert!(cache.get("tenant-a", &query).await.unwrap() == None);

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
