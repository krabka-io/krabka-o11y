use super::*;

#[tokio::test]
pub(crate) async fn vector_vector_arithmetic_drops_metadata_labels() {
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
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "requests_total"),
            ("__type__", "counter"),
            ("__unit__", "requests"),
            ("instance", "b"),
        ]),
        10_000,
        5.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "requests_total + 1", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
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
