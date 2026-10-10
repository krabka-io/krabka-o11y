use super::*;

#[tokio::test]
pub(crate) async fn instant_changes_counts_value_transitions_in_range() {
    let store = SeriesFixture::new(labels(&[("__name__", "queue_depth"), ("job", "api")]))
        .at(0_i64, 1.0)
        .at(60_000, 1.0)
        .at(120_000, 2.0)
        .at(180_000, 2.0)
        .at(240_000, 5.0)
        .store();

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "changes(queue_depth[4m])", 240_000).await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        2.0,
    );
}
