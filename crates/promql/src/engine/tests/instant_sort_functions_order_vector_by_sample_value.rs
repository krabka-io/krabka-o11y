use super::*;

#[tokio::test]
pub(crate) async fn instant_sort_functions_order_vector_by_sample_value() {
    let mut store = InMemoryMetricStore::new();
    for (instance, zone, value) in [
        ("api-b", "us-west-2b", 3.0),
        ("api-a", "us-east-1a", 1.0),
        ("api-c", "us-east-1a", 2.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "queue_depth"),
                ("instance", instance),
                ("zone", zone),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected_instances) in [
        ("sort(queue_depth)", ["api-a", "api-c", "api-b"]),
        ("sort_desc(queue_depth)", ["api-b", "api-c", "api-a"]),
        (
            r#"sort_by_label(queue_depth, "zone", "instance")"#,
            ["api-a", "api-c", "api-b"],
        ),
        (
            r#"sort_by_label_desc(queue_depth, "zone", "instance")"#,
            ["api-b", "api-c", "api-a"],
        ),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert2::assert!(samples.len() == 3);
        let instances = samples
            .iter()
            .map(|sample| sample.labels.get("instance").unwrap().to_string())
            .collect::<Vec<_>>();
        assert2::assert!(instances == expected_instances);
        assert2::assert!(
            samples
                .iter()
                .all(|sample| sample.labels.get("__name__") == Some("queue_depth"))
        );
    }
}
