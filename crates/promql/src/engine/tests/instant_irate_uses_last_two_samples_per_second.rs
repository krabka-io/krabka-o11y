use super::*;

#[tokio::test]
pub(crate) async fn instant_irate_uses_last_two_samples_per_second() {
    let store = SeriesFixture::new(labels(&[
        ("__name__", "http_requests_total"),
        ("job", "api"),
    ]))
    .at(0_i64, 0.0)
    .at(60_000, 1.0)
    .at(90_000, 3.0)
    .store();

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "irate(http_requests_total[2m])", 90_000).await;
    assert2::assert!(samples.len() == 1);
    assert2::assert!(approx_eq(float_value(&samples[0].value), 2.0 / 30.0));
}
