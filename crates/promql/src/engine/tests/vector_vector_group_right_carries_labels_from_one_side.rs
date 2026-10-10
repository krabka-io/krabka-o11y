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
    let samples = instant_vector(
        &engine,
        "target_limit / on (job) group_right(region) http_requests_total",
        10_000,
    )
    .await;
    check!(samples.len() == 2);
    let has_east_api_sample = |instance: &str, want: f64| {
        samples.iter().any(|sample| {
            sample.labels.get("__name__").is_none()
                && sample.labels.get("job") == Some("api")
                && sample.labels.get("region") == Some("east")
                && sample.labels.get("instance") == Some(instance)
                && approx_eq(float_value(&sample.value), want)
        })
    };
    check!(has_east_api_sample("a", 10.0));
    check!(has_east_api_sample("b", 4.0));
}
