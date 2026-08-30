use super::*;

#[tokio::test]
pub(crate) async fn instant_time_returns_evaluation_timestamp_seconds() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "time()", 123_456)
        .await
        .unwrap();

    assert2::assert!(
        result
            == QueryResult::Scalar {
                ts_ms: 123_456,
                value: 123.456
            }
    );
}
