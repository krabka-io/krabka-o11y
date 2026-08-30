use super::*;

#[cfg(not(feature = "experimental-functions"))]
#[tokio::test]
pub(crate) async fn instant_limitk_requires_experimental_feature() {
    let store = InMemoryMetricStore::new();
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let error = engine
        .query_instant("tenant-a", "limitk(2, memory_bytes)", 10_000)
        .await
        .unwrap_err();

    assert2::assert!(matches!(error, PromqlError::Unsupported(_)));
    assert2::assert!(format!("{error}").contains("experimental-functions"));
}
