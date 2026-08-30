use super::*;

#[test]
pub(crate) fn range_result_cache_returns_owned_results() {
    let cache = QueryFrontendCache::default();
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

    cache.insert("tenant-a", &query, result);
    let Some(QueryResult::RangeMatrix(mut first_hit)) = cache.get("tenant-a", &query) else {
        panic!("cached range matrix");
    };
    first_hit[0].samples.clear();

    let Some(QueryResult::RangeMatrix(second_hit)) = cache.get("tenant-a", &query) else {
        panic!("cached range matrix");
    };
    assert2::assert!(second_hit[0].samples == vec![(0, SampleValue::Float(1.0))]);
}
