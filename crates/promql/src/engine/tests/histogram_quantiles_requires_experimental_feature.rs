use super::*;

#[cfg(not(feature = "experimental-functions"))]
#[tokio::test]
pub(crate) async fn histogram_quantiles_requires_experimental_feature() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let error = engine
        .query_instant(
            "tenant-a",
            r#"histogram_quantiles(vector(1), "quantile", 0.5)"#,
            10_000,
        )
        .await
        .unwrap_err();

    assert2::assert!(format!("{error}").contains("experimental-functions"));
}
