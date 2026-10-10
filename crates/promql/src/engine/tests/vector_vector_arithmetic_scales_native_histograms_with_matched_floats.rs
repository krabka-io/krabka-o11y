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
        assert_on_x_histogram_stats(
            &engine,
            OnXHistogramStats {
                query,
                count: expected_count,
                sum: expected_sum,
            },
        )
        .await;
    }

    let samples =
        instant_vector(&engine, "histogram_count(factor / on (x) duration)", 10_000).await;
    assert2::assert!(samples.is_empty());
}
