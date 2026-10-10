use super::*;

#[tokio::test]
pub(crate) async fn instant_over_time_accepts_subquery_range_argument() {
    let store = SeriesFixture::new(labels(&[("__name__", "queue_depth"), ("job", "api")]))
        .at(0_i64, 1.0)
        .at(60_000, 2.0)
        .at(120_000, 3.0)
        .store();

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "avg_over_time((queue_depth * 2)[2m:1m])", 120_000).await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        5.0,
    );
}
