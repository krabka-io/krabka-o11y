use super::*;

#[tokio::test]
pub(crate) async fn instant_count_and_group_aggregations_include_histograms() {
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

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        ("count by (job) (mixed_metric)", 2.0),
        ("group by (job) (mixed_metric)", 1.0),
    ] {
        let samples = instant_vector(&engine, query, 10_000).await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            expected,
        );
    }
}
