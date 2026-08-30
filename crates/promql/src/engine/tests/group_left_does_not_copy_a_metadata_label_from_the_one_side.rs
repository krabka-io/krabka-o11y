use super::*;

/// `group_left (...)` copies the named labels off the one side. A metadata
/// label named there must still be skipped, or every result row inherits the
/// *other* operand's metric name.
#[tokio::test]
pub(crate) async fn group_left_does_not_copy_a_metadata_label_from_the_one_side() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("1", 3.0), ("2", 4.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "a"), ("job", "x"), ("instance", instance)]),
            10_000,
            value,
        );
    }
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "b"), ("job", "x")]),
        10_000,
        10.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "a + on (job) group_left(__name__) b", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 2);
    for sample in &samples {
        assert2::assert!(sample.labels.get("__name__") == None);
    }
}
