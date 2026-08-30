use super::*;

/// Averaging native histograms sums them and scales by one over the count.
/// Every other scaling in these tests uses a factor of -1, where multiplying
/// and dividing agree -- a half does not.
#[tokio::test]
pub(crate) async fn avg_over_time_scales_a_native_histogram_by_one_over_the_count() {
    let histogram = |value: f64| NativeHistogram {
        schema: 0,
        is_float: true,
        reset_hint: ResetHint::No,
        zero_threshold: 1.0,
        zero_count: value,
        count: value,
        sum: value,
        positive_spans: vec![],
        positive_counts: vec![],
        negative_spans: vec![],
        negative_counts: vec![],
        custom_values: None,
        start_timestamp_ms: None,
    };
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "h")]),
        10_000,
        histogram(4.0),
    );
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "h")]),
        20_000,
        histogram(6.0),
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let QueryResult::InstantVector(samples) = engine
        .query_instant("tenant-a", "avg_over_time(h[5m])", 20_000)
        .await
        .expect("a histogram average")
    else {
        panic!("expected a vector");
    };
    let SampleValue::Histogram(averaged) = &samples[0].value else {
        panic!("expected a histogram sample");
    };
    check!(
        approx_eq(averaged.zero_count, 5.0),
        "the zero bucket averages"
    );
    check!(approx_eq(averaged.count, 5.0));
    check!(approx_eq(averaged.sum, 5.0));
}
