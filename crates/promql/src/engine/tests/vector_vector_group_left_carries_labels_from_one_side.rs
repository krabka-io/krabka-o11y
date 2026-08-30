use super::*;

#[tokio::test]
pub(crate) async fn vector_vector_group_left_carries_labels_from_one_side() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 100.0), ("b", 50.0)] {
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
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "target_info"),
            ("job", "api"),
            ("region", "east"),
        ]),
        10_000,
        10.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "http_requests_total / on (job) group_left(region) target_info",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 2);
    for sample in samples {
        check!(sample.labels.get("__name__").is_none());
        check!(sample.labels.get("job") == Some("api"));
        check!(sample.labels.get("region") == Some("east"));
    }
}
