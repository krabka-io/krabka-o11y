use super::*;

#[tokio::test]
pub(crate) async fn in_memory_without_ttl_never_expires() {
    let clock = Arc::new(ManualClock::new(0));
    let cache = QueryFrontendCache::default().with_clock(clock.clone());
    let query = up_range_query(0, None);
    let result = one_sample_matrix(labels(&[("__name__", "up")]));

    cache
        .insert("tenant-a", &query, result.clone())
        .await
        .unwrap();
    clock.advance(i64::from(u32::MAX));
    assert2::assert!(cache.get("tenant-a", &query).await.unwrap() == Some(result));
}
