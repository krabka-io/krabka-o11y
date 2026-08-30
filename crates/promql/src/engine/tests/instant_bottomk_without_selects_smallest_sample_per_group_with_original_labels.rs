use super::*;

#[tokio::test]
pub(crate) async fn instant_bottomk_without_selects_smallest_sample_per_group_with_original_labels()
{
    let mut store = InMemoryMetricStore::new();
    for (job, instance, value) in [
        ("api", "a", 4.0),
        ("api", "b", 1.0),
        ("worker", "c", 5.0),
        ("worker", "d", 2.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "memory_bytes"),
                ("job", job),
                ("instance", instance),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "bottomk without (instance) (1, memory_bytes)",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 2);
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("memory_bytes")
            && sample.labels.get("job") == Some("api")
            && sample.labels.get("instance") == Some("b")
            && approx_eq(float_value(&sample.value), 1.0)
    }));
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("memory_bytes")
            && sample.labels.get("job") == Some("worker")
            && sample.labels.get("instance") == Some("d")
            && approx_eq(float_value(&sample.value), 2.0)
    }));
}
