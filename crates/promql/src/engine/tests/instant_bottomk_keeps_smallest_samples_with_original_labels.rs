use super::*;

#[tokio::test]
pub(crate) async fn instant_bottomk_keeps_smallest_samples_with_original_labels() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 1.0), ("b", 3.0), ("c", 2.0)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "memory_bytes"), ("instance", instance)]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "bottomk(2, memory_bytes)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 2);
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("memory_bytes")
            && sample.labels.get("instance") == Some("a")
            && approx_eq(float_value(&sample.value), 1.0)
    }));
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("memory_bytes")
            && sample.labels.get("instance") == Some("c")
            && approx_eq(float_value(&sample.value), 2.0)
    }));
}
