use super::*;

#[tokio::test]
pub(crate) async fn vector_and_default_set_matching_ignores_metadata_labels() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "requests_total"),
            ("__type__", "counter"),
            ("__unit__", "requests"),
            ("instance", "a"),
        ]),
        10_000,
        10.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "(requests_total + 1) and requests_total", 10_000).await;
    check!(samples.len() == 1);
    check!(samples[0].value == SampleValue::Float(11.0));
}
