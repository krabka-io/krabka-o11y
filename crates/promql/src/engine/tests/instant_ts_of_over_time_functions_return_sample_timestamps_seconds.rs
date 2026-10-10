use super::*;

#[tokio::test]
pub(crate) async fn instant_ts_of_over_time_functions_return_sample_timestamps_seconds() {
    let store = SeriesFixture::new(labels(&[("__name__", "queue_depth"), ("job", "api")]))
        .at(0_i64, 10.0)
        .at(60_000, 3.0)
        .at(120_000, 7.0)
        .at(180_000, 3.0)
        .at(240_000, 11.0)
        .store();

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        ("ts_of_first_over_time(queue_depth[4m])", 60.0),
        ("ts_of_last_over_time(queue_depth[4m])", 240.0),
        ("ts_of_min_over_time(queue_depth[4m])", 180.0),
        ("ts_of_max_over_time(queue_depth[4m])", 240.0),
    ] {
        let samples = instant_vector(&engine, query, 240_000).await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            expected,
        );
    }
}
