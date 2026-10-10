use super::*;

#[tokio::test]
pub(crate) async fn instant_irate_uses_last_two_samples_per_second() {
    let engine = late_jump_engine("http_requests_total");
    let samples = instant_vector(&engine, "irate(http_requests_total[2m])", 90_000).await;
    assert2::assert!(samples.len() == 1);
    assert2::assert!(approx_eq(float_value(&samples[0].value), 2.0 / 30.0));
}
