use super::*;

#[cfg(not(feature = "experimental-functions"))]
#[tokio::test]
pub(crate) async fn instant_duration_expression_helpers_require_experimental_feature() {
    let store = InMemoryMetricStore::new();
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for query in ["range()", "step()", "start()", "end()"] {
        let error = engine
            .query_instant("tenant-a", query, 120_000)
            .await
            .unwrap_err();

        assert2::assert!(matches!(error, PromqlError::Unsupported(_)));
        assert2::assert!(format!("{error}").contains("experimental-functions"));
    }
}
