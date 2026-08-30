use super::*;

#[tokio::test]
pub(crate) async fn instant_label_join_combines_source_labels() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "up"),
            ("job", "api"),
            ("instance", "a"),
            ("zone", "us-east-1a"),
        ]),
        10_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            r#"label_join(up, "target", "/", "job", "instance")"#,
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(samples[0].labels.get("target") == Some("api/a"));
    assert2::assert!(samples[0].labels.get("zone") == Some("us-east-1a"));
    assert2::assert!(approx_eq(float_value(&samples[0].value), 1.0));
}
