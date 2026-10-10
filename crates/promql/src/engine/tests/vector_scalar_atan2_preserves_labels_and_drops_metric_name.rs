use super::*;

#[tokio::test]
pub(crate) async fn vector_scalar_atan2_preserves_labels_and_drops_metric_name() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "y"), ("job", "api")]),
        10_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "y atan2 0", 10_000).await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        std::f64::consts::FRAC_PI_2,
    );
}
