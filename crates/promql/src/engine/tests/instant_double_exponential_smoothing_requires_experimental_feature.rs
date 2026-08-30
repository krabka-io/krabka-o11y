use super::*;

#[cfg(not(feature = "experimental-functions"))]
#[tokio::test]
pub(crate) async fn instant_double_exponential_smoothing_requires_experimental_feature() {
    let store = InMemoryMetricStore::new();
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let error = engine
        .query_instant(
            "tenant-a",
            "double_exponential_smoothing(gauge[5m], 0.5, 0.5)",
            120_000,
        )
        .await
        .unwrap_err();

    assert2::assert!(matches!(error, PromqlError::Unsupported(_)));
    assert2::assert!(format!("{error}").contains("experimental-functions"));
}
