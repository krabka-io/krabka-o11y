use super::*;

#[tokio::test]
pub(crate) async fn instant_topk_and_bottomk_ignore_histograms() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 1.0), ("b", 3.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "memory_bytes"), ("instance", instance)]),
            10_000,
            value,
        );
    }
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "memory_bytes"), ("instance", "hist")]),
        10_000,
        native_histogram(4.0, 10.0),
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected_instance, expected_value) in [
        ("topk(1, memory_bytes)", "b", 3.0),
        ("bottomk(1, memory_bytes)", "a", 1.0),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert2::assert!(samples.len() == 1);
        assert2::assert!(samples[0].labels.get("__name__") == Some("memory_bytes"));
        assert2::assert!(samples[0].labels.get("instance") == Some(expected_instance));
        assert2::assert!(approx_eq(float_value(&samples[0].value), expected_value));
    }
}
