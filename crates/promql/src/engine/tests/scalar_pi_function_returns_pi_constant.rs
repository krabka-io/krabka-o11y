use super::*;

#[tokio::test]
pub(crate) async fn scalar_pi_function_returns_pi_constant() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "pi()", 10_000)
        .await
        .unwrap();
    assert2::assert!(
        result
            == QueryResult::Scalar {
                ts_ms: 10_000,
                value: std::f64::consts::PI,
            }
    );
}
