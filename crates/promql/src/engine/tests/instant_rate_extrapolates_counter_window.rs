use super::*;

#[tokio::test]
pub(crate) async fn instant_rate_extrapolates_counter_window() {
    let store = SeriesFixture::new(labels(&[
        ("__name__", "http_requests_total"),
        ("job", "api"),
    ]))
    .at(0_i64, 0.0)
    .at(60_000, 1.0)
    .at(120_000, 2.0)
    .at(180_000, 3.0)
    .at(240_000, 4.0)
    .store();

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "rate(http_requests_total[5m])", 300_000).await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        5.0 / 300.0,
    );
}
