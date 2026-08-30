use super::*;

#[test]
pub(crate) fn in_memory_round_trips_then_expires() {
    let clock = Arc::new(ManualClock::new(1_000_000));
    let cache = QueryFrontendCache::with_ttl(secs(90)).with_clock(clock.clone());
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

    cache.insert("tenant-a", &query, result.clone());

    // Within the TTL window: hit.
    clock.advance(89_000);
    assert2::assert!(cache.get("tenant-a", &query) == Some(result));

    // One step past the TTL: miss, and the entry is evicted.
    clock.advance(2_000);
    assert2::assert!(cache.get("tenant-a", &query) == None);
    assert2::assert!(
        cache
            .range_results
            .lock()
            .expect("query frontend cache poisoned")
            .len()
            == 0
    );
}
