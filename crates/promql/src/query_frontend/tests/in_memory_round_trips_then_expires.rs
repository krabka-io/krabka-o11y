use super::*;

#[tokio::test]
pub(crate) async fn in_memory_round_trips_then_expires() {
    let clock = Arc::new(ManualClock::new(1_000_000));
    let cache = QueryFrontendCache::with_ttl(secs(90)).with_clock(clock.clone());
    let query = up_range_query(0, None);
    let result = one_sample_matrix(labels(&[("__name__", "up")]));

    cache
        .insert("tenant-a", &query, result.clone())
        .await
        .unwrap();

    // Within the TTL window: hit.
    clock.advance(89_000);
    assert2::assert!(cache.get("tenant-a", &query).await.unwrap() == Some(result));

    // One step past the TTL: miss, and the entry is evicted.
    clock.advance(2_000);
    assert2::assert!(cache.get("tenant-a", &query).await.unwrap() == None);
}
