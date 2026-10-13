use super::*;

#[tokio::test]
pub(crate) async fn info_function_keeps_base_label_when_info_label_overlaps() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        super::target_info_engine::request_metric_labels(&[("region", "base")]),
        10_000,
        7.0,
    );
    store.push_float(
        "tenant-a",
        super::target_info_engine::info_series_labels(
            "target_info",
            &[("region", "info"), ("cluster", "prod")],
        ),
        10_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "info(http_requests_total)", 10_000).await;
    assert2::assert!(samples.len() == 1);
    assert2::assert!(samples[0].labels.get("region") == Some("base"));
    assert2::assert!(samples[0].labels.get("cluster") == Some("prod"));
}
