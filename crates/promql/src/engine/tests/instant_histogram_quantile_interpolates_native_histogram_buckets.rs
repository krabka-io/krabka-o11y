use super::*;

#[tokio::test]
pub(crate) async fn instant_histogram_quantile_interpolates_native_histogram_buckets() {
    let store = two_bucket_histogram_store(6.5);

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(
        &engine,
        "histogram_quantile(0.5, request_duration_seconds)",
        10_000,
    )
    .await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        2_f64.powf(1.0 / 3.0),
    );
}
