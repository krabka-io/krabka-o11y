use super::*;

#[tokio::test]
pub(crate) async fn vector_vector_group_left_fill_right_preserves_unmatched_many_side() {
    let mut store = InMemoryMetricStore::new();
    for (job, instance, value) in [
        ("api", "a", 100.0),
        ("api", "b", 50.0),
        ("worker", "c", 7.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "http_requests_total"),
                ("job", job),
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
            "http_requests_total + on (job) group_left(region) fill_right(0) target_info",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    let values = samples
        .iter()
        .map(|sample| {
            (
                sample.labels.get("instance").expect("instance label"),
                (sample.labels.get("region"), float_value(&sample.value)),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert2::assert!(values.len() == 3);
    assert2::assert!(values["a"].0 == Some("east"));
    assert2::assert!(approx_eq(values["a"].1, 110.0));
    assert2::assert!(values["b"].0 == Some("east"));
    assert2::assert!(approx_eq(values["b"].1, 60.0));
    assert2::assert!(values["c"].0 == None);
    assert2::assert!(approx_eq(values["c"].1, 7.0));
}
