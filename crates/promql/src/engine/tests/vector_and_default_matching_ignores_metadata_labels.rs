use super::*;

#[tokio::test]
pub(crate) async fn vector_and_default_matching_ignores_metadata_labels() {
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
    let result = engine
        .query_instant(
            "tenant-a",
            "(requests_total + 1) and requests_total",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("__type__").is_none());
    check!(samples[0].labels.get("__unit__").is_none());
    check!(samples[0].labels.get("instance") == Some("a"));
    check!(approx_eq(float_value(&samples[0].value), 11.0));
}
