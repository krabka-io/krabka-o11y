use super::*;

#[tokio::test]
pub(crate) async fn instant_histogram_count_returns_native_histogram_count() {
    let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
    let samples =
        instant_vector(&engine, "histogram_count(request_duration_seconds)", 10_000).await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        4.0,
    );
}
