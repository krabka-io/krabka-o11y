use super::*;

#[tokio::test]
pub(crate) async fn instant_sum_by_groups_by_exact_labels_and_drops_metric_name() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "b")]),
        10_000,
        2.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web"), ("instance", "c")]),
        10_000,
        4.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "sum by (job) (up)", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 2);
    let api = samples
        .iter()
        .find(|sample| sample.labels.get("job") == Some("api"))
        .expect("api group");
    assert2::assert!(api.labels.get("__name__") == None);
    assert2::assert!(api.labels.get("instance") == None);
    assert2::assert!(approx_eq(float_value(&api.value), 3.0));
    let web = samples
        .iter()
        .find(|sample| sample.labels.get("job") == Some("web"))
        .expect("web group");
    assert2::assert!(approx_eq(float_value(&web.value), 4.0));
}
