use super::*;

#[tokio::test]
pub(crate) async fn vector_vector_arithmetic_matches_on_labels() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "a"), ("x", "1")]),
        10_000,
        10.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "b"), ("x", "1")]),
        10_000,
        5.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "b"), ("x", "2")]),
        10_000,
        99.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "a + on (x) b", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("x") == Some("1"));
    check!(approx_eq(float_value(&samples[0].value), 15.0));
}
