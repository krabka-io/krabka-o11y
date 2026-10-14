use super::*;

#[tokio::test]
pub(crate) async fn instant_changes_counts_value_transitions_in_range() {
    let store = queue_depth_store(&[
        (0, 1.0),
        (60_000, 1.0),
        (120_000, 2.0),
        (180_000, 2.0),
        (240_000, 5.0),
    ]);

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
