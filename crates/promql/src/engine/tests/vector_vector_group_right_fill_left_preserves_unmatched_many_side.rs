use super::*;

#[tokio::test]
pub(crate) async fn vector_vector_group_right_fill_left_preserves_unmatched_many_side() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "job_quota"),
            ("job", "api"),
            ("region", "east"),
        ]),
        10_000,
        10.0,
    );
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

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "job_quota + on (job) group_right(region) fill_left(0) http_requests_total",
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
