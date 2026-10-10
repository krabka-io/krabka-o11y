use super::*;

#[tokio::test]
pub(crate) async fn instant_selector_stale_marker_terminates_series_before_lookback_expiry() {
    let store = SeriesFixture::new(labels(&[("__name__", "up"), ("job", "api")]))
        .at(10_000, 1.0)
        .at(20_000, stale_nan())
        .store();
    let engine = lookback_engine(store, millis(60_000));
    let samples = instant_vector(&engine, "up", 30_000).await;
    assert2::assert!(samples.is_empty());
}
