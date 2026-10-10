use super::*;

#[tokio::test]
pub(crate) async fn instant_selector_at_and_offset_combine_order_independently() {
    let engine = short_lookback_up_engine();
    for query in ["up @ 120 offset 1m", "up offset 1m @ 120"] {
        let samples = instant_vector(&engine, query, 999_000).await;
        check!(samples.len() == 1);
        check!(samples[0].ts_ms == 999_000);
        check!(approx_eq(float_value(&samples[0].value), 1.0));
    }
}
