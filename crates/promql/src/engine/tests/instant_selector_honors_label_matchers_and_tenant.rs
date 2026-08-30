use super::*;

#[tokio::test]
pub(crate) async fn instant_selector_honors_label_matchers_and_tenant() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web")]),
        10_000,
        0.0,
    );
    store.push_float(
        "tenant-b",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        9.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", r#"up{job=~"a.*"}"#, 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 1.0));
}
