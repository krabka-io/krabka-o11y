use super::*;

#[tokio::test]
pub(crate) async fn scalar_binary_arithmetic_returns_scalar() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "2 * 3", 10_000)
        .await
        .unwrap();
    assert2::assert!(
        result
            == QueryResult::Scalar {
                ts_ms: 10_000,
                value: 6.0
            }
    );
}
