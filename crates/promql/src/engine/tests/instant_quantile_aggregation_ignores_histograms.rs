use super::*;

#[tokio::test]
pub(crate) async fn instant_quantile_aggregation_ignores_histograms() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 2.0), ("b", 6.0)] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "latency_seconds"),
                ("job", "api"),
                ("instance", instance),
            ]),
            10_000,
            value,
        );
    }
    store.push_histogram(
        "tenant-a",
        labels(&[
            ("__name__", "latency_seconds"),
            ("job", "api"),
            ("instance", "hist"),
        ]),
        10_000,
        native_histogram(4.0, 10.0),
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "quantile by (job) (0.5, latency_seconds)", 10_000).await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        4.0,
    );
}
