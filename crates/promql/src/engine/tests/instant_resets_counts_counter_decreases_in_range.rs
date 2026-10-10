use super::*;

#[tokio::test]
pub(crate) async fn instant_resets_counts_counter_decreases_in_range() {
    let store = SeriesFixture::new(labels(&[
        ("__name__", "http_requests_total"),
        ("job", "api"),
    ]))
    .at(0_i64, 0.0)
    .at(60_000, 5.0)
    .at(120_000, 1.0)
    .at(180_000, 4.0)
    .at(240_000, 2.0)
    .store();

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "resets(http_requests_total[4m])", 240_000).await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        2.0,
    );
}
