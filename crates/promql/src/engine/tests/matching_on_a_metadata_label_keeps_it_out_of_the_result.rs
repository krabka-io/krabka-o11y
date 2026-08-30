use super::*;

/// `on (...)` builds the result labels from the clause itself, so naming a
/// metadata label there would otherwise carry `__name__` into the output of an
/// arithmetic expression -- which never has one.
#[tokio::test]
pub(crate) async fn matching_on_a_metadata_label_keeps_it_out_of_the_result() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "x"), ("job", "a")]),
        10_000,
        7.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "x - on (__name__) x", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(samples[0].labels.iter().count() == 0);
    assert2::assert!(approx_eq(float_value(&samples[0].value), 0.0));
}
