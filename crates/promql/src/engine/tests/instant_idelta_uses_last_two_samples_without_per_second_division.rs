use super::*;

#[tokio::test]
pub(crate) async fn instant_idelta_uses_last_two_samples_without_per_second_division() {
    let engine = late_jump_engine("temperature_celsius");
    let samples = instant_vector(&engine, "idelta(temperature_celsius[2m])", 90_000).await;
    assert2::assert!(samples.len() == 1);
    assert2::assert!(approx_eq(float_value(&samples[0].value), 2.0));
}
