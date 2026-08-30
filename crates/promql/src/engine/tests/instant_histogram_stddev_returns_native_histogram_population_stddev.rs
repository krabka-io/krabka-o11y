use super::*;

#[tokio::test]
pub(crate) async fn instant_histogram_stddev_returns_native_histogram_population_stddev() {
    let mut histogram = native_histogram(4.0, 5.25);
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
            "histogram_stddev(request_duration_seconds)",
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
    check!(approx_eq(
        float_value(&samples[0].value),
        0.099_384_473_924_297_3_f64.sqrt()
    ));
}
