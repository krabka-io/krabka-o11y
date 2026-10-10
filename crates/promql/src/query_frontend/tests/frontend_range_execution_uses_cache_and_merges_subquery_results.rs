use super::*;

#[tokio::test]
pub(crate) async fn frontend_range_execution_uses_cache_and_merges_subquery_results() {
    let cache = QueryFrontendCache::default();
    let executor = RecordingExecutor::default();
    let cached_query = up_range_query(60_000, None);
    cache
        .insert(
            "tenant-a",
            &cached_query,
            one_sample_matrix(labels(&[("__name__", "up"), ("job", "api")])),
        )
        .await
        .unwrap();

    let result = execute_range_query_frontend(
        &executor,
        &cache,
        &FrontendRangeRequest {
            tenant: tenant_id("tenant-a"),
            query: "up".into(),
            start_ms: 0,
            end_ms: 180_000,
            step: millis(60_000),
            admission_limits: krabka_query_frontend::AdmissionLimits::default(),
            opts: QueryFrontendOptions {
                split_interval: millis(120_000),
                shard_count: 1,
            },
        },
    )
    .await
    .unwrap();

    // Absolute windows [0,120k)->[0,60k] (pre-cached) and
    // [120k,240k)->[120k,180k] (executed fresh).
    let calls = executor
        .calls
        .lock()
        .expect("recording executor calls poisoned")
        .clone();
    assert2::assert!(
        calls
            .iter()
            .map(|query| (query.start_ms, query.end_ms))
            .collect::<Vec<_>>()
            == vec![(120_000, 180_000)]
    );
    assert2::assert!(
        cache
            .get("tenant-a", &calls[0])
            .await
            .unwrap()
            .expect("fresh subquery cached")
            == up_api_matrix(&[FloatPoint {
                ts_ms: 120_000,
                value: 120_000.0
            }])
    );
    assert2::assert!(
        result
            == up_api_matrix(&[
                FloatPoint {
                    ts_ms: 0,
                    value: 1.0
                },
                FloatPoint {
                    ts_ms: 120_000,
                    value: 120_000.0
                }
            ])
    );
}
