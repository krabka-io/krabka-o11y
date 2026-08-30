use super::*;

#[tokio::test]
pub(crate) async fn instant_histogram_fraction_estimates_native_histogram_bucket_overlap() {
    let mut histogram = native_histogram(4.0, 6.5);
    histogram.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 2,
    }];
    histogram.positive_counts = vec![1.0, 3.0];
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "request_duration_seconds"), ("job", "api")]),
        10_000,
        histogram,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            "histogram_fraction(1, 2, request_duration_seconds)",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 0.75));
}
