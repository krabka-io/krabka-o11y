use super::*;

#[test]
pub(crate) fn in_memory_without_ttl_never_expires() {
    let clock = Arc::new(ManualClock::new(0));
    let cache = QueryFrontendCache::default().with_clock(clock.clone());
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
    clock.advance(i64::from(u32::MAX));
    assert2::assert!(cache.get("tenant-a", &query) == Some(result));
}
