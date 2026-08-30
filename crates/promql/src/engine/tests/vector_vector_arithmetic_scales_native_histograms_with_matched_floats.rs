use super::*;

#[tokio::test]
pub(crate) async fn vector_vector_arithmetic_scales_native_histograms_with_matched_floats() {
    let mut histogram = native_histogram(4.0, 10.0);
    histogram.zero_count = 1.0;
    histogram.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 1,
    }];
    histogram.positive_counts = vec![3.0];

    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "duration"), ("job", "api"), ("x", "1")]),
        10_000,
        histogram,
    );
    store.push_float(
        "tenant-a",
        labels(&[("__name__", "factor"), ("job", "api"), ("x", "1")]),
        10_000,
        2.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected_count, expected_sum) in [
        ("duration * on (x) factor", 8.0, 20.0),
        ("factor * on (x) duration", 8.0, 20.0),
        ("duration / on (x) factor", 2.0, 5.0),
    ] {
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

    let invalid = engine
        .query_instant(
            "tenant-a",
            "histogram_count(factor / on (x) duration)",
            10_000,
        )
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = invalid else {
        panic!("expected vector");
    };
    assert2::assert!(samples.is_empty());
}
