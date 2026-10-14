use super::*;

#[derive(bon::Builder)]
struct HistogramAggregateExpectation {
    query: &'static str,
    count: f64,
    sum: f64,
    average: f64,
}

#[tokio::test]
pub(crate) async fn instant_sum_and_avg_aggregations_combine_compatible_native_histograms() {
    let mut left = native_histogram(4.0, 10.0);
    left.zero_count = 1.0;
    left.positive_spans = vec![BucketSpan {
        offset: 0,
        length: 2,
    }];
    left.positive_counts = vec![1.0, 2.0];
    let mut right = native_histogram(6.0, 20.0);
    right.zero_count = 2.0;
    right.positive_spans = left.positive_spans.clone();
    right.positive_counts = vec![2.0, 2.0];

    let store = instance_histogram_store(InstanceHistograms { a: left, b: right });

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for expected in [
        HistogramAggregateExpectation::builder()
            .query("sum by (job) (request_duration_seconds)")
            .count(10.0)
            .sum(30.0)
            .average(3.0)
            .build(),
        HistogramAggregateExpectation::builder()
            .query("avg by (job) (request_duration_seconds)")
            .count(5.0)
            .sum(15.0)
            .average(3.0)
            .build(),
    ] {
        let count = engine
            .query_instant(
                &tenant_id("tenant-a"),
                &format!("histogram_count({})", expected.query),
                10_000,
            )
            .await
            .unwrap();
        let sum = engine
            .query_instant(
                &tenant_id("tenant-a"),
                &format!("histogram_sum({})", expected.query),
                10_000,
            )
            .await
            .unwrap();
        let avg = engine
            .query_instant(
                &tenant_id("tenant-a"),
                &format!("histogram_avg({})", expected.query),
                10_000,
            )
            .await
            .unwrap();

        assert_single_float_sample(&count, "api", expected.count, expected.query);
        assert_single_float_sample(&sum, "api", expected.sum, expected.query);
        assert_single_float_sample(&avg, "api", expected.average, expected.query);
    }
}
