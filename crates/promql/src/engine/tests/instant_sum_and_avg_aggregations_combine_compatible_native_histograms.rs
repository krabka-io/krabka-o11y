use super::*;

#[tokio::test]
pub(crate) async fn instant_sum_and_avg_aggregations_combine_compatible_native_histograms() {
    let mut left = native_histogram(4.0, 10.0);
    left.zero_count = 1.0;
    left.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 2,
    }];
    left.positive_counts = vec![1.0, 2.0];
    let mut right = native_histogram(6.0, 20.0);
    right.zero_count = 2.0;
    right.positive_spans = left.positive_spans.clone();
    right.positive_counts = vec![2.0, 2.0];

    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[
            ("__name__", "request_duration_seconds"),
            ("job", "api"),
            ("instance", "a"),
        ]),
        10_000,
        left,
    );
    store.push_histogram(
        "tenant-a",
        labels(&[
            ("__name__", "request_duration_seconds"),
            ("job", "api"),
            ("instance", "b"),
        ]),
        10_000,
        right,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected_count, expected_sum, expected_avg) in [
        ("sum by (job) (request_duration_seconds)", 10.0, 30.0, 3.0),
        ("avg by (job) (request_duration_seconds)", 5.0, 15.0, 3.0),
    ] {
        let count = engine
            .query_instant("tenant-a", &format!("histogram_count({query})"), 10_000)
            .await
            .unwrap();
        let sum = engine
            .query_instant("tenant-a", &format!("histogram_sum({query})"), 10_000)
            .await
            .unwrap();
        let avg = engine
            .query_instant("tenant-a", &format!("histogram_avg({query})"), 10_000)
            .await
            .unwrap();

        assert_single_float_sample(&count, "api", expected_count, query);
        assert_single_float_sample(&sum, "api", expected_sum, query);
        assert_single_float_sample(&avg, "api", expected_avg, query);
    }
}
