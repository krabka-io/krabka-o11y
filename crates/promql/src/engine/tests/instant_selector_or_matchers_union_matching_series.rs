use super::*;

#[tokio::test]
pub(crate) async fn instant_selector_or_matchers_union_matching_series() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
        10_000,
        1.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
        10_000,
        2.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "db"), ("instance", "c")]),
        10_000,
        3.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", r#"up{job="api" or job="web"}"#, 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 2);
    let values_by_job = samples
        .iter()
        .map(|sample| {
            (
                sample.labels.get("job").expect("job label").to_string(),
                float_value(&sample.value),
            )
        })
        .collect::<BTreeMap<_, _>>();
    check!(approx_eq(values_by_job["api"], 1.0));
    check!(approx_eq(values_by_job["web"], 2.0));
    check!(!values_by_job.contains_key("db"));
}
