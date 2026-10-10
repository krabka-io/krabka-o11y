use super::*;

#[tokio::test]
pub(crate) async fn instant_idelta_uses_last_two_samples_without_per_second_division() {
    let store = SeriesFixture::new(labels(&[
        ("__name__", "temperature_celsius"),
        ("job", "api"),
    ]))
    .at(0_i64, 0.0)
    .at(60_000, 1.0)
    .at(90_000, 3.0)
    .store();

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "idelta(temperature_celsius[2m])", 90_000).await;
    assert2::assert!(samples.len() == 1);
    assert2::assert!(approx_eq(float_value(&samples[0].value), 2.0));
}
