use super::*;

#[tokio::test]
pub(crate) async fn vector_vector_arithmetic_drops_metadata_labels() {
    let mut store = InMemoryMetricStore::new();
    push_metadata_sample(&mut store, "a", 10.0);
    push_metadata_sample(&mut store, "b", 5.0);

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "requests_total + 1", 10_000).await;
    check!(samples.len() == 2);
    for name in ["__name__", "__type__", "__unit__"] {
        check!(
            samples
                .iter()
                .all(|sample| sample.labels.get(name).is_none()),
            "{name} must be dropped"
        );
    }
}
