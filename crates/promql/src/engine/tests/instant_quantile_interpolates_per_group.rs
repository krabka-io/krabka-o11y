use super::*;

#[tokio::test]
pub(crate) async fn instant_quantile_interpolates_per_group() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [1.0, 2.0, 4.0, 8.0].into_iter().enumerate() {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "latency_seconds"),
                ("job", "api"),
                ("instance", &instance.to_string()),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "quantile by (job) (0.5, latency_seconds)", 10_000).await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        3.0,
    );
}
