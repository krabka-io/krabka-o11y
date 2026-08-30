use super::*;

#[tokio::test]
pub(crate) async fn instant_sum_aggregation_downscales_native_histograms() {
    let mut left = native_histogram(4.0, 10.0);
    left.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 1,
    }];
    left.positive_counts = vec![1.0];
    let mut right = native_histogram(6.0, 20.0);
    right.schema = 1;
    right.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 1,
    }];
    right.positive_counts = vec![2.0];

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
    let result = engine
        .query_instant(
            "tenant-a",
            "sum by (job) (request_duration_seconds)",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected instant vector");
    };
    assert2::assert!(samples.len() == 1);
    let SampleValue::Histogram(histogram) = &samples[0].value else {
        panic!("expected histogram");
    };
    assert2::assert!(histogram.schema == 0);
    assert2::assert!((histogram.count - 10.0).abs() < f64::EPSILON);
    assert2::assert!((histogram.sum - 30.0).abs() < f64::EPSILON);
    assert2::assert!(&histogram.positive_counts == &vec![3.0]);
}
