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
    let samples = instant_vector(
        &engine,
        "http_requests_total + on (job) group_left(region) fill_right(0) target_info",
        10_000,
    )
    .await;
    assert_filled_many_side(&samples);
}
