use super::*;

#[tokio::test]
pub(crate) async fn native_histogram_scalar_arithmetic_scales_histograms() {
    let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
    for (query, expected) in [
        ("histogram_count(request_duration_seconds * 2)", 8.0),
        ("histogram_sum(2 * request_duration_seconds)", 20.0),
        ("histogram_count(request_duration_seconds / 2)", 2.0),
        ("histogram_sum(request_duration_seconds / 2)", 5.0),
    ] {
        let samples = instant_vector(&engine, query, 10_000).await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            expected,
        );
    }
}
