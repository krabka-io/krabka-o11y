use super::*;

#[tokio::test]
pub(crate) async fn instant_histogram_avg_returns_native_histogram_average() {
    let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
    let samples = instant_vector(&engine, "histogram_avg(request_duration_seconds)", 10_000).await;
    check!(samples.len() == 1);
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 2.5));
}
