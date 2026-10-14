use super::{native_histogram_store::single_bucket_instance_histogram, *};

#[tokio::test]
pub(crate) async fn instant_sum_aggregation_downscales_native_histograms() {
    let left = single_bucket_instance_histogram(4.0, 10.0, 0, 0, 1.0);
    let right = single_bucket_instance_histogram(6.0, 20.0, 1, 0, 2.0);

    let store = instance_histogram_store(InstanceHistograms { a: left, b: right });

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            &tenant_id("tenant-a"),
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
