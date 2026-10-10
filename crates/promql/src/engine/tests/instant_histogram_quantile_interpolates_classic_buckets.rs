use super::*;

#[tokio::test]
pub(crate) async fn instant_histogram_quantile_interpolates_classic_buckets() {
    let engine = PromqlEngine::new(Arc::new(classic_bucket_store()), EngineOpts::default());
    let samples = instant_vector(
        &engine,
        "histogram_quantile(0.5, http_request_duration_seconds_bucket)",
        10_000,
    )
    .await;
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("le").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 0.25));
}
