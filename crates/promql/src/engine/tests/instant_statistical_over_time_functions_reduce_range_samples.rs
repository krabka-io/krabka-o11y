use super::*;

#[tokio::test]
pub(crate) async fn instant_statistical_over_time_functions_reduce_range_samples() {
    let store = SeriesFixture::new(labels(&[("__name__", "latency_seconds"), ("job", "api")]))
        .at(0_i64, 2.0)
        .at(60_000, 4.0)
        .at(120_000, 4.0)
        .at(180_000, 4.0)
        .at(240_000, 5.0)
        .at(300_000, 5.0)
        .at(360_000, 7.0)
        .at(420_000, 9.0)
        .store();

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        ("stdvar_over_time(latency_seconds[8m])", 4.0),
        ("stddev_over_time(latency_seconds[8m])", 2.0),
        ("quantile_over_time(0.5, latency_seconds[8m])", 4.5),
        ("mad_over_time(latency_seconds[8m])", 0.5),
    ] {
        let samples = instant_vector(&engine, query, 420_000).await;
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
