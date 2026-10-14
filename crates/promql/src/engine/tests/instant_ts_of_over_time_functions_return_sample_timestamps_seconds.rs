use super::*;

struct TimestampFunctionExpectation {
    query: &'static str,
    timestamp_seconds: f64,
}

#[tokio::test]
pub(crate) async fn instant_ts_of_over_time_functions_return_sample_timestamps_seconds() {
    let store = SeriesFixture::new(labels(&[
        crate::test_support::TestLabelPair::new("__name__", "queue_depth"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]))
    .at(0_i64, 10.0)
    .at(60_000, 3.0)
    .at(120_000, 7.0)
    .at(180_000, 3.0)
    .at(240_000, 11.0)
    .store();

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for expectation in [
        TimestampFunctionExpectation {
            query: "ts_of_first_over_time(queue_depth[4m])",
            timestamp_seconds: 60.0,
        },
        TimestampFunctionExpectation {
            query: "ts_of_last_over_time(queue_depth[4m])",
            timestamp_seconds: 240.0,
        },
        TimestampFunctionExpectation {
            query: "ts_of_min_over_time(queue_depth[4m])",
            timestamp_seconds: 180.0,
        },
        TimestampFunctionExpectation {
            query: "ts_of_max_over_time(queue_depth[4m])",
            timestamp_seconds: 240.0,
        },
    ] {
        let samples = instant_vector(&engine, expectation.query, 240_000).await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            expectation.timestamp_seconds,
        );
    }
}
