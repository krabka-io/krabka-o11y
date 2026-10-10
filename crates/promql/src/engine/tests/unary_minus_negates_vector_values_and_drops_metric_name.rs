use super::*;

#[tokio::test]
pub(crate) async fn unary_minus_negates_vector_values_and_drops_metric_name() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "temperature_celsius"), ("job", "api")]),
        10_000,
        3.5,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "-temperature_celsius", 10_000).await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        -3.5,
    );
}
