use super::*;

#[tokio::test]
pub(crate) async fn instant_topk_keeps_largest_samples_with_original_labels() {
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
        .query_instant("tenant-a", "topk(2, memory_bytes)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    let mut projection = samples
        .iter()
        .map(|sample| {
            (
                sample.labels.get("__name__"),
                sample.labels.get("instance"),
                float_value(&sample.value),
            )
        })
        .collect::<Vec<_>>();
    projection.sort_by_key(|(_, instance, _)| *instance);
    check!(
        projection
            == vec![
                (Some("memory_bytes"), Some("b"), 3.0),
                (Some("memory_bytes"), Some("c"), 2.0),
            ]
    );
}
