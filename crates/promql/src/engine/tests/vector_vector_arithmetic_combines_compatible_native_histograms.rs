use super::*;

#[tokio::test]
pub(crate) async fn vector_vector_arithmetic_combines_compatible_native_histograms() {
    let mut left = native_histogram(4.0, 10.0);
    left.zero_count = 1.0;
    left.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 1,
    }];
    left.positive_counts = vec![3.0];
    let mut right = native_histogram(2.0, 4.0);
    right.zero_count = 0.5;
    right.positive_spans = left.positive_spans.clone();
    right.positive_counts = vec![1.5];

    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "a"), ("job", "api"), ("x", "1")]),
        10_000,
        left,
    );
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "b"), ("job", "api"), ("x", "1")]),
        10_000,
        right,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected_count, expected_sum) in
        [("a + on (x) b", 6.0, 14.0), ("a - on (x) b", 2.0, 6.0)]
    {
        let count = engine
            .query_instant("tenant-a", &format!("histogram_count({query})"), 10_000)
            .await
            .unwrap();
        let sum = engine
            .query_instant("tenant-a", &format!("histogram_sum({query})"), 10_000)
            .await
            .unwrap();

        assert_single_on_x_float_sample(&count, expected_count, query);
        assert_single_on_x_float_sample(&sum, expected_sum, query);
    }
}
