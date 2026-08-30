use super::*;

#[tokio::test]
pub(crate) async fn instant_quantile_aggregation_ignores_histograms() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("a", 2.0), ("b", 6.0)] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "latency_seconds"),
                ("job", "api"),
                ("instance", instance),
            ]),
            10_000,
            value,
        );
    }
    store.push_histogram(
        "tenant-a",
        labels(&[
            ("__name__", "latency_seconds"),
            ("job", "api"),
            ("instance", "hist"),
        ]),
        10_000,
        native_histogram(4.0, 10.0),
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "quantile by (job) (0.5, latency_seconds)",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(samples[0].labels.get("__name__") == None);
    assert2::assert!(samples[0].labels.get("job") == Some("api"));
    assert2::assert!(approx_eq(float_value(&samples[0].value), 4.0));
}
