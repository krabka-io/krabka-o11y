use super::*;

#[tokio::test]
pub(crate) async fn instant_histogram_fraction_estimates_native_histogram_bucket_overlap() {
    let store = two_bucket_histogram_store(6.5);

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(
        &engine,
        "histogram_fraction(1, 2, request_duration_seconds)",
        10_000,
    )
    .await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        0.75,
    );
}
