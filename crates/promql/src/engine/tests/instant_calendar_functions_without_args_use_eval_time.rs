use super::*;

#[tokio::test]
pub(crate) async fn instant_calendar_functions_without_args_use_eval_time() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "minute()", 3_660_000)
        .await
        .unwrap();

    assert2::assert!(
        result
            == QueryResult::Scalar {
                ts_ms: 3_660_000,
                value: 1.0,
            }
    );
}
