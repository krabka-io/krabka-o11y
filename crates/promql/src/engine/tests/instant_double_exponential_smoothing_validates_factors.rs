use super::*;

#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn instant_double_exponential_smoothing_validates_factors() {
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(0_i64, 3.0), (60_000, 6.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "queue_depth"), ("job", "api")]),
            ts_ms,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let error = engine
        .query_instant(
            "tenant-a",
            "double_exponential_smoothing(queue_depth[2m], 1, 0.5)",
            60_000,
        )
        .await
        .unwrap_err();

    assert2::assert!(matches!(error, PromqlError::Plan(_)));
    assert2::assert!(format!("{error}").contains("smoothing factor"));
}
