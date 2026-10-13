use super::*;

#[tokio::test]
pub(crate) async fn in_memory_round_trips_then_expires() {
    let clock = Arc::new(ManualClock::new(1_000_000));
    let cache = QueryFrontendCache::with_ttl(secs(90)).with_clock(clock.clone());
    let fixture = up_query_and_result();
    let query = fixture.query;
    let result = fixture.result;

    assert_ttl_hit_then_miss(&cache, &clock, &query, &result, 89_000, 2_000).await;
}
