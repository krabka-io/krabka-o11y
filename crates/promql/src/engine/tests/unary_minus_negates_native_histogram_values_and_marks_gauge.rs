use super::*;

#[tokio::test]
pub(crate) async fn unary_minus_negates_native_histogram_values_and_marks_gauge() {
    let mut histogram = native_histogram(4.0, 10.0);
    histogram.reset_hint = ResetHint::No;
    histogram.zero_count = 1.0;
    histogram.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 1,
    }];
    histogram.positive_counts = vec![3.0];

    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
        10_000,
        histogram,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "-request_duration_seconds", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
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
