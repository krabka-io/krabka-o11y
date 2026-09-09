use super::*;

/// Prometheus sends a `NaN` to the bottom under `sort` and under `sort_desc`.
/// A `NaN` is neither the largest value nor the smallest one, so no direction
/// puts it first. The `sort_by_label` family orders by label and never reads
/// the value, so the `NaN` series takes the position its labels give it.
#[tokio::test]
pub(crate) async fn instant_sort_functions_place_nan_last() {
    let mut store = InMemoryMetricStore::new();
    for (instance, zone, value) in [
        ("api-b", "us-west-2b", 3.0),
        ("api-a", "us-east-1a", 1.0),
        ("api-c", "us-east-1a", 2.0),
        ("api-n", "us-east-1a", f64::NAN),
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
        ("sort(queue_depth)", ["api-a", "api-c", "api-b", "api-n"]),
        (
            "sort_desc(queue_depth)",
            ["api-b", "api-c", "api-a", "api-n"],
        ),
        (
            r#"sort_by_label(queue_depth, "zone", "instance")"#,
            ["api-a", "api-c", "api-n", "api-b"],
        ),
        (
            r#"sort_by_label_desc(queue_depth, "zone", "instance")"#,
            ["api-b", "api-n", "api-c", "api-a"],
        ),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        let instances = samples
            .iter()
            .map(|sample| sample.labels.get("instance").unwrap())
            .collect::<Vec<_>>();
        assert2::assert!(instances == expected_instances);
        let nan_position = instances
            .iter()
            .position(|instance| *instance == "api-n")
            .unwrap();
        assert2::assert!(
            matches!(samples[nan_position].value, SampleValue::Float(value) if value.is_nan())
        );
    }
}
