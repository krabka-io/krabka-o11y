#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn limit_ratio_over_bound_emits_capping_warning() {
    let mut store = InMemoryMetricStore::new();
    for instance in ["0", "1"] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "http_requests"), ("instance", instance)]),
            0,
            1.0,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let (_, annotations) = engine
        .query_instant_with_annotations("tenant-a", "count(limit_ratio(1.1, http_requests))", 0)
        .await
        .expect("query");
    assert2::assert!(
        annotations.warnings
            == vec![
                "PromQL warning: ratio value should be between -1 and 1, got 1.1, capping to 1"
                    .to_string()
            ]
    );
}
