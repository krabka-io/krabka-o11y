use super::*;

#[tokio::test]
pub(crate) async fn instant_label_replace_uses_regex_capture_groups() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("instance", "api-1:9100")]),
        10_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            r#"label_replace(up, "host", "$1", "instance", "([^:]+):.*")"#,
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(samples[0].labels.get("host") == Some("api-1"));
    assert2::assert!(samples[0].labels.get("instance") == Some("api-1:9100"));
    assert2::assert!(approx_eq(float_value(&samples[0].value), 1.0));
}
