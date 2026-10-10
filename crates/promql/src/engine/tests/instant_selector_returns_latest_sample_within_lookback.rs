use super::*;

#[tokio::test]
pub(crate) async fn instant_selector_returns_latest_sample_within_lookback() {
    let store = SeriesFixture::new(labels(&[("__name__", "up"), ("job", "api")]))
        .at(10_000, 1.0)
        .at(20_000, 2.0)
        .at(40_000, 4.0)
        .store();
    let engine = lookback_engine(store, millis(15_000));

    let samples = instant_vector(&engine, "up", 30_000).await;
    check!(
        (
            samples.len(),
            &samples[0].labels,
            samples[0].ts_ms,
            approx_eq(float_value(&samples[0].value), 2.0),
        ) == (
            1,
            &crate::PromqlLabels::from(labels(&[("__name__", "up"), ("job", "api")])),
            30_000,
            true,
        )
    );
}
