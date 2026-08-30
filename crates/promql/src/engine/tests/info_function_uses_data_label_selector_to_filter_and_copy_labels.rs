use super::*;

#[tokio::test]
pub(crate) async fn info_function_uses_data_label_selector_to_filter_and_copy_labels() {
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
        .query_instant(
            "tenant-a",
            r#"info(http_requests_total, {region="east"})"#,
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(
        samples[0].labels.clone()
            == labels(&[
                ("__name__", "http_requests_total"),
                ("job", "api"),
                ("instance", "a"),
                ("region", "east"),
            ])
    );
    assert2::assert!(approx_eq(float_value(&samples[0].value), 7.0));
}
