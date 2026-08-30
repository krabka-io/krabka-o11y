use super::*;

#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn instant_limitk_by_selects_deterministic_hash_subset_per_group() {
    let mut store = InMemoryMetricStore::new();
    for (job, instance, value) in [
        ("api", "a", 1.0),
        ("api", "b", 2.0),
        ("api", "c", 3.0),
        ("worker", "d", 4.0),
        ("worker", "e", 5.0),
        ("worker", "f", 6.0),
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
        .query_instant("tenant-a", "limitk by (job) (1, memory_bytes)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 2);
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("memory_bytes")
            && sample.labels.get("job") == Some("api")
            && sample.labels.get("instance") == Some("c")
            && approx_eq(float_value(&sample.value), 3.0)
    }));
    let selected = sample_instances(&samples);
    check!(selected == vec!["c", "e"]);
}
