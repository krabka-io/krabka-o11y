use super::*;

#[tokio::test]
pub(crate) async fn instant_absent_returns_one_with_equality_matcher_labels_when_vector_is_empty() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            r#"absent(up{job="worker",instance=~".*"})"#,
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("worker"));
    check!(samples[0].labels.get("instance").is_none());
    check!(approx_eq(float_value(&samples[0].value), 1.0));
}
