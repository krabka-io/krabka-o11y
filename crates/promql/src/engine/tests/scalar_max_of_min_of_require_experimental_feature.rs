use super::*;

#[cfg(not(feature = "experimental-functions"))]
#[tokio::test]
pub(crate) async fn scalar_max_of_min_of_require_experimental_feature() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    for query in ["max_of(1, 2)", "min_of(1, 2)"] {
        let error = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap_err();

        assert2::assert!(matches!(error, PromqlError::Unsupported(_)));
        assert2::assert!(format!("{error}").contains("experimental-functions"));
    }
}
