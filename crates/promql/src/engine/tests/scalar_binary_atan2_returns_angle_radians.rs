use super::*;

#[tokio::test]
pub(crate) async fn scalar_binary_atan2_returns_angle_radians() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "1 atan2 1", 10_000)
        .await
        .unwrap();
    assert2::assert!(
        result
            == QueryResult::Scalar {
                ts_ms: 10_000,
                value: std::f64::consts::FRAC_PI_4,
            }
    );
}
