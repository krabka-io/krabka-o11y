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
            tenant: tenant_id("tenant-a"),
            query: "up".into(),
            start_ms: 0,
            end_ms: 120_000,
            step: millis(60_000),
            admission_limits: krabka_query_frontend::AdmissionLimits::default(),
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
            == up_api_matrix(&[
                FloatPoint {
                    ts_ms: 0,
                    value: 1.0
                },
                FloatPoint {
                    ts_ms: 60_000,
                    value: 2.0
                },
                FloatPoint {
                    ts_ms: 120_000,
                    value: 3.0
                }
            ])
    );
}
