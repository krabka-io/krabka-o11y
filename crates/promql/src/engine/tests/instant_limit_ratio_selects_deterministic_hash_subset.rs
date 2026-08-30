
#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn instant_limit_ratio_selects_deterministic_hash_subset() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0), ("e", 5.0)] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "memory_bytes"),
                ("job", "api"),
                ("instance", instance),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "limit_ratio(0.75, memory_bytes)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    let selected = sample_instances(&samples);
    assert2::assert!(selected == vec!["a", "b", "c", "d"]);
}
