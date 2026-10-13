use super::*;

struct AggregationExpectation {
    query: &'static str,
    expected: f64,
}

#[tokio::test]
pub(crate) async fn instant_min_max_and_std_aggregations_ignore_histograms() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "mixed_metric"),
            crate::test_support::TestLabelPair::new("job", "api"),
            crate::test_support::TestLabelPair::new("instance", "a"),
        ]),
        10_000,
        4.0,
    );
    store.push_float(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "mixed_metric"),
            crate::test_support::TestLabelPair::new("job", "api"),
            crate::test_support::TestLabelPair::new("instance", "b"),
        ]),
        10_000,
        8.0,
    );
    store.push_histogram(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "mixed_metric"),
            crate::test_support::TestLabelPair::new("job", "api"),
            crate::test_support::TestLabelPair::new("instance", "hist"),
        ]),
        10_000,
        native_histogram(4.0, 10.0),
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
