use super::*;

#[tokio::test]
pub(crate) async fn vector_vector_group_right_carries_labels_from_one_side() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "target_limit"),
            ("job", "api"),
            ("region", "east"),
        ]),
        10_000,
        100.0,
    );
    for (instance, value) in [("a", 10.0), ("b", 25.0)] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "http_requests_total"),
                ("job", "api"),
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
            "target_limit / on (job) group_right(region) http_requests_total",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 2);
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__").is_none()
            && sample.labels.get("job") == Some("api")
            && sample.labels.get("region") == Some("east")
            && sample.labels.get("instance") == Some("a")
            && approx_eq(float_value(&sample.value), 10.0)
    }));
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__").is_none()
            && sample.labels.get("job") == Some("api")
            && sample.labels.get("region") == Some("east")
            && sample.labels.get("instance") == Some("b")
            && approx_eq(float_value(&sample.value), 4.0)
    }));
}
