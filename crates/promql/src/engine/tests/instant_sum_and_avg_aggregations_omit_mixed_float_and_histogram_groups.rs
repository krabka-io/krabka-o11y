use super::*;

#[tokio::test]
pub(crate) async fn instant_sum_and_avg_aggregations_omit_mixed_float_and_histogram_groups() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "mixed_metric"),
            ("job", "api"),
            ("instance", "float"),
        ]),
        10_000,
        4.0,
    );
    store.push_histogram(
        "tenant-a",
        labels(&[
            ("__name__", "mixed_metric"),
            ("job", "api"),
            ("instance", "hist"),
        ]),
        10_000,
        native_histogram(4.0, 10.0),
    );
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "mixed_metric"),
            ("job", "web"),
            ("instance", "float"),
        ]),
        10_000,
        6.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for query in ["sum by (job) (mixed_metric)", "avg by (job) (mixed_metric)"] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();

        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert2::assert!(samples.len() == 1);
        assert2::assert!(samples[0].labels.get("job") == Some("web"));
        assert2::assert!(approx_eq(float_value(&samples[0].value), 6.0));
    }
}
