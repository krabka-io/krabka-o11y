use super::*;

#[tokio::test]
pub(crate) async fn instant_label_join_combines_source_labels() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "up"),
            crate::test_support::TestLabelPair::new("job", "api"),
            crate::test_support::TestLabelPair::new("instance", "a"),
            crate::test_support::TestLabelPair::new("zone", "us-east-1a"),
        ]),
        10_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            &tenant_id("tenant-a"),
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
