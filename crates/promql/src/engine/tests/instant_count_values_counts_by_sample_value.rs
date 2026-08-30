use super::*;

#[tokio::test]
pub(crate) async fn instant_count_values_counts_by_sample_value() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [200.0, 200.0, 500.0].into_iter().enumerate() {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "http_responses_total"),
                ("job", "api"),
                ("instance", &instance.to_string()),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            r#"count_values("code", http_responses_total)"#,
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 2);
    let ok = samples
        .iter()
        .find(|sample| sample.labels.get("code") == Some("200"))
        .expect("200 bucket");
    assert2::assert!(ok.labels.get("__name__") == None);
    assert2::assert!(approx_eq(float_value(&ok.value), 2.0));
    let err = samples
        .iter()
        .find(|sample| sample.labels.get("code") == Some("500"))
        .expect("500 bucket");
    assert2::assert!(approx_eq(float_value(&err.value), 1.0));
}
