use super::*;

#[tokio::test]
pub(crate) async fn instant_delta_is_gauge_delta_without_reset_correction() {
    let store = SeriesFixture::new(labels(&[
        ("__name__", "temperature_celsius"),
        ("job", "api"),
    ]))
    .at(30_000_i64, 4.0)
    .at(60_000, 3.0)
    .store();

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "delta(temperature_celsius[1m])", 60_000).await;
    assert2::assert!(samples.len() == 1);
    assert2::assert!(approx_eq(float_value(&samples[0].value), -2.0));
}
