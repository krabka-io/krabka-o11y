use super::*;

#[tokio::test]
pub(crate) async fn vector_vector_arithmetic_drops_metadata_labels() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "requests_total"),
            crate::test_support::TestLabelPair::new("__type__", "counter"),
            crate::test_support::TestLabelPair::new("__unit__", "requests"),
            crate::test_support::TestLabelPair::new("instance", "a"),
        ]),
        10_000,
        10.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "requests_total"),
            crate::test_support::TestLabelPair::new("__type__", "counter"),
            crate::test_support::TestLabelPair::new("__unit__", "requests"),
            crate::test_support::TestLabelPair::new("instance", "b"),
        ]),
        10_000,
        5.0,
    );

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
