use super::*;

#[tokio::test]
pub(crate) async fn info_function_adds_target_info_data_labels_by_job_and_instance() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "http_requests_total"),
            ("job", "api"),
            ("instance", "a"),
        ]),
        10_000,
        7.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "target_info"),
            ("job", "api"),
            ("instance", "a"),
            ("region", "east"),
            ("cluster", "prod"),
        ]),
        10_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "info(http_requests_total)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__") == Some("http_requests_total"));
    check!(samples[0].labels.get("job") == Some("api"));
    check!(samples[0].labels.get("instance") == Some("a"));
    check!(samples[0].labels.get("region") == Some("east"));
    check!(samples[0].labels.get("cluster") == Some("prod"));
    check!(approx_eq(float_value(&samples[0].value), 7.0));
}
