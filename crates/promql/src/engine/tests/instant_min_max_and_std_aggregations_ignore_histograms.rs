use super::*;

struct AggregationExpectation {
    query: &'static str,
    expected: f64,
}

#[tokio::test]
pub(crate) async fn instant_min_max_and_std_aggregations_ignore_histograms() {
    let store = mixed_metric_store(
        "mixed_metric",
        &[
            MixedMetricFloatSample::builder()
                .instance("a")
                .value(4.0)
                .build(),
            MixedMetricFloatSample::builder()
                .instance("b")
                .value(8.0)
                .build(),
        ],
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for expectation in [
        AggregationExpectation {
            query: "min by (job) (mixed_metric)",
            expected: 4.0,
        },
        AggregationExpectation {
            query: "max by (job) (mixed_metric)",
            expected: 8.0,
        },
        AggregationExpectation {
            query: "stddev by (job) (mixed_metric)",
            expected: 2.0,
        },
        AggregationExpectation {
            query: "stdvar by (job) (mixed_metric)",
            expected: 4.0,
        },
    ] {
        let samples = instant_vector(&engine, expectation.query, 10_000).await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            expectation.expected,
        );
    }
}
