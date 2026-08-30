use super::*;

#[test]
pub(crate) fn range_result_cache_is_scoped_by_tenant_query_range_step_and_shard() {
    let cache = QueryFrontendCache::default();
    let query = FrontendRangeQuery {
        query: "up".into(),
        start_ms: 0,
        end_ms: 60_000,
        step: millis(60_000),
        shard: Some(QueryShard { index: 1, total: 2 }),
    };
    let result = QueryResult::RangeMatrix(vec![RangeSeries {
        labels: labels(&[("__name__", "up"), ("job", "api")]),
        samples: vec![(0, SampleValue::Float(1.0))],
    }]);

    cache.insert("tenant-a", &query, result.clone());

    assert2::assert!(cache.get("tenant-a", &query) == Some(result));
    assert2::assert!(cache.get("tenant-b", &query) == None);

    let other_shard = FrontendRangeQuery {
        shard: Some(QueryShard { index: 2, total: 2 }),
        ..query
    };
    assert2::assert!(cache.get("tenant-a", &other_shard) == None);
}
