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
    // Prometheus swaps the operand sides for group_right before applying fill flags.
    let samples = instant_vector(
        &engine,
        "job_quota + on (job) group_right(region) fill_right(0) http_requests_total",
        10_000,
    )
    .await;
    assert_filled_many_side(&samples);
}
