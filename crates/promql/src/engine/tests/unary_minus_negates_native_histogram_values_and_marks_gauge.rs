use super::*;

#[tokio::test]
pub(crate) async fn unary_minus_negates_native_histogram_values_and_marks_gauge() {
    let histogram = single_bucket_native_histogram(4.0, 10.0, 1.0, 3.0);

    let store = super::native_histogram_store::store_with_request_duration_histogram(histogram);

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "-request_duration_seconds", 10_000).await;
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    let SampleValue::Histogram(histogram) = &samples[0].value else {
        panic!("expected histogram");
    };
    check!(histogram.reset_hint == ResetHint::Gauge);
    check!(approx_eq(histogram.count, -4.0));
    check!(approx_eq(histogram.sum, -10.0));
    check!(approx_eq(histogram.zero_count, -1.0));
    check!(
        histogram
            .positive_counts
            .iter()
            .any(|count| approx_eq(*count, -3.0))
    );
}
