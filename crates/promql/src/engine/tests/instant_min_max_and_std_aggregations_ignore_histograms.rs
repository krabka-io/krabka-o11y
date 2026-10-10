use super::*;

#[tokio::test]
pub(crate) async fn instant_min_max_and_std_aggregations_ignore_histograms() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "mixed_metric"),
            ("job", "api"),
            ("instance", "a"),
        ]),
        10_000,
        4.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[
            ("__name__", "mixed_metric"),
            ("job", "api"),
            ("instance", "b"),
        ]),
        10_000,
        8.0,
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
        ("min by (job) (mixed_metric)", 4.0),
        ("max by (job) (mixed_metric)", 8.0),
        ("stddev by (job) (mixed_metric)", 2.0),
        ("stdvar by (job) (mixed_metric)", 4.0),
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
