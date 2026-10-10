use super::*;

#[tokio::test]
pub(crate) async fn instant_selector_offset_shifts_evaluation_time_backwards() {
    let engine = short_lookback_up_engine();
    let samples = instant_vector(&engine, "up offset 1m", 120_000).await;
    check!(samples.len() == 1);
    check!(samples[0].ts_ms == 120_000);
    check!(approx_eq(float_value(&samples[0].value), 1.0));
}
