use super::*;

#[tokio::test]
pub(crate) async fn frontend_range_execution_runs_against_promql_engine() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        0,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        60_000,
        2.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        120_000,
        3.0,
    );
    let engine = PromqlEngine::new(std::sync::Arc::new(store), EngineOpts::default());
    let cache = QueryFrontendCache::default();

    let result = execute_range_query_frontend(
        &engine,
        &cache,
        &FrontendRangeRequest {
            tenant: "tenant-a".into(),
            query: "up".into(),
            start_ms: 0,
            end_ms: 120_000,
            step: millis(60_000),
            opts: QueryFrontendOptions {
                split_interval: millis(60_000),
                shard_count: 1,
            },
        },
    )
    .await
    .unwrap();

    assert2::assert!(
        result
            == QueryResult::RangeMatrix(vec![RangeSeries {
                labels: labels(&[("__name__", "up"), ("job", "api")]),
                samples: vec![
                    (0, SampleValue::Float(1.0)),
                    (60_000, SampleValue::Float(2.0)),
                    (120_000, SampleValue::Float(3.0)),
                ],
            }])
    );
}
