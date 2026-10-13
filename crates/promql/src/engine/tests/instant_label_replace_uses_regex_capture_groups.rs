use super::*;

#[tokio::test]
pub(crate) async fn instant_label_replace_uses_regex_capture_groups() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "up"),
            crate::test_support::TestLabelPair::new("instance", "api-1:9100"),
        ]),
        10_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    assert_label_transform(
        &engine,
        r#"label_replace(up, "host", "$1", "instance", "([^:]+):.*")"#,
        "host",
        "api-1",
        "instance",
        "api-1:9100",
    )
    .await;
}
