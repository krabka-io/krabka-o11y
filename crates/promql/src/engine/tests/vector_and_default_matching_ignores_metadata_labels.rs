use super::*;

#[tokio::test]
pub(crate) async fn vector_and_default_set_matching_ignores_metadata_labels() {
    let mut store = InMemoryMetricStore::new();
    push_metadata_sample(&mut store, "a", 10.0);

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "(requests_total + 1) and requests_total", 10_000).await;
    check!(samples.len() == 1);
    check!(samples[0].value == SampleValue::Float(11.0));
}
