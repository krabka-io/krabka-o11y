use super::*;

#[derive(bon::Builder)]
struct HistogramScaleExpectation {
    query: &'static str,
    count: f64,
    sum: f64,
}

#[tokio::test]
pub(crate) async fn vector_vector_arithmetic_scales_native_histograms_with_matched_floats() {
    let histogram = single_bucket_native_histogram(4.0, 10.0, 1.0, 3.0);

    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "duration"),
            crate::test_support::TestLabelPair::new("job", "api"),
            crate::test_support::TestLabelPair::new("x", "1"),
        ]),
        10_000,
        histogram,
    );
    store.push_float(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "factor"),
            crate::test_support::TestLabelPair::new("job", "api"),
            crate::test_support::TestLabelPair::new("x", "1"),
        ]),
        10_000,
        2.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for expectation in [
        HistogramScaleExpectation::builder()
            .query("duration * on (x) factor")
            .count(8.0)
            .sum(20.0)
            .build(),
        HistogramScaleExpectation::builder()
            .query("factor * on (x) duration")
            .count(8.0)
            .sum(20.0)
            .build(),
        HistogramScaleExpectation::builder()
            .query("duration / on (x) factor")
            .count(2.0)
            .sum(5.0)
            .build(),
    ] {
        assert_on_x_histogram_stats(
            &engine,
            OnXHistogramStats {
                query: expectation.query,
                count: expectation.count,
                sum: expectation.sum,
            },
        )
        .await;
    }

    let samples =
        instant_vector(&engine, "histogram_count(factor / on (x) duration)", 10_000).await;
    assert2::assert!(samples.is_empty());
}
