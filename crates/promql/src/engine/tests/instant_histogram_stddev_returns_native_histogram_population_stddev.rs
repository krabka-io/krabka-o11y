use super::*;

#[tokio::test]
pub(crate) async fn instant_histogram_stddev_returns_native_histogram_population_stddev() {
    let store = two_bucket_histogram_store(5.25);

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(
        &engine,
        "histogram_stddev(request_duration_seconds)",
        10_000,
    )
    .await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        0.099_384_473_924_297_3_f64.sqrt(),
    );
}
