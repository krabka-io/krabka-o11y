use super::*;

struct StatisticalExpectation {
    query: &'static str,
    value: f64,
}

#[tokio::test]
pub(crate) async fn instant_statistical_over_time_functions_reduce_range_samples() {
    let store = SeriesFixture::new(labels(&[
        crate::test_support::TestLabelPair::new("__name__", "latency_seconds"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]))
    .at(0_i64, 2.0)
    .at(60_000, 4.0)
    .at(120_000, 4.0)
    .at(180_000, 4.0)
    .at(240_000, 5.0)
    .at(300_000, 5.0)
    .at(360_000, 7.0)
    .at(420_000, 9.0)
    .store();

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for expectation in [
        StatisticalExpectation {
            query: "stdvar_over_time(latency_seconds[8m])",
            value: 4.0,
        },
        StatisticalExpectation {
            query: "stddev_over_time(latency_seconds[8m])",
            value: 2.0,
        },
        StatisticalExpectation {
            query: "quantile_over_time(0.5, latency_seconds[8m])",
            value: 4.5,
        },
        StatisticalExpectation {
            query: "mad_over_time(latency_seconds[8m])",
            value: 0.5,
        },
    ] {
        let samples = instant_vector(&engine, expectation.query, 420_000).await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            expectation.value,
        );
    }
}
