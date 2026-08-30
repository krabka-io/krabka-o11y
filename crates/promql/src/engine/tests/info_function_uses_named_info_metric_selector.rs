use super::*;

#[tokio::test]
pub(crate) async fn info_function_uses_named_info_metric_selector() {
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
            ("__name__", "build_info"),
            ("job", "api"),
            ("instance", "a"),
            ("version", "1.2.3"),
        ]),
        10_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            r#"info(http_requests_total, {__name__="build_info"})"#,
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(samples[0].labels.get("version") == Some("1.2.3"));
}
