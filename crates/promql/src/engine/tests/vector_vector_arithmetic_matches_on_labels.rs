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
    let samples = instant_vector(&engine, "a + on (x) b", 10_000).await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "x",
            label_value: "1",
        },
        15.0,
    );
}
