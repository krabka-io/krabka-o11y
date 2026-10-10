use super::*;

#[tokio::test]
pub(crate) async fn instant_sum_aggregation_combines_native_histograms_with_different_span_layouts()
{
    let mut left = native_histogram(4.0, 10.0);
    left.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 1,
    }];
    left.positive_counts = vec![1.0];
    let mut right = native_histogram(6.0, 20.0);
    right.positive_spans = vec![BucketSpan {
        offset: 1,
        length: 1,
    }];
    right.positive_counts = vec![2.0];

    let store = instance_histogram_store(InstanceHistograms { a: left, b: right });

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "sum by (job) (request_duration_seconds)", 10_000).await;
    assert2::assert!(samples.len() == 1);
    let SampleValue::Histogram(histogram) = &samples[0].value else {
        panic!("expected histogram");
    };
    assert2::assert!(approx_eq(histogram.count, 10.0));
    assert2::assert!(approx_eq(histogram.sum, 30.0));
    assert2::assert!(
        &histogram.positive_spans
            == &vec![BucketSpan {
                offset: 0,
                length: 2,
            }]
    );
    assert2::assert!(&histogram.positive_counts == &vec![1.0, 2.0]);
}
