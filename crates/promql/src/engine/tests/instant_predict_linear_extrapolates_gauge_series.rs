use super::*;

#[tokio::test]
pub(crate) async fn instant_predict_linear_extrapolates_gauge_series() {
    let store = SeriesFixture::new(labels(&[("__name__", "disk_free_bytes"), ("job", "api")]))
        .at(0_i64, 1.0)
        .at(60_000, 3.0)
        .at(120_000, 5.0)
        .store();

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "predict_linear(disk_free_bytes[2m], 60)", 120_000).await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        7.0,
    );
    // The same series over a window that admits all three samples. `[2m]`
    // starts exactly at the first one, and the window excludes its own start,
    // so only two ever reached the regression -- leaving a guard that refused
    // three samples with nothing to refuse.
    let QueryResult::InstantVector(samples) = engine
        .query_instant(
            &tenant_id("tenant-a"),
            "predict_linear(disk_free_bytes[3m], 60)",
            120_000,
        )
        .await
        .expect("a prediction")
    else {
        panic!("expected a vector");
    };
    check!(samples.len() == 1, "three samples still predict");
    check!(approx_eq(float_value(&samples[0].value), 7.0));
}
