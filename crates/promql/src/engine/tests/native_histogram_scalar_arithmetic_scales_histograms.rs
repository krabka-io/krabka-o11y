use super::*;

struct QueryExpectation {
    query: &'static str,
    value: f64,
}

#[tokio::test]
pub(crate) async fn native_histogram_scalar_arithmetic_scales_histograms() {
    let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
    for expectation in [
        QueryExpectation {
            query: "histogram_count(request_duration_seconds * 2)",
            value: 8.0,
        },
        QueryExpectation {
            query: "histogram_sum(2 * request_duration_seconds)",
            value: 20.0,
        },
        QueryExpectation {
            query: "histogram_count(request_duration_seconds / 2)",
            value: 2.0,
        },
        QueryExpectation {
            query: "histogram_sum(request_duration_seconds / 2)",
            value: 5.0,
        },
    ] {
        let samples = instant_vector(&engine, expectation.query, 10_000).await;
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
