use super::*;

#[tokio::test]
pub(crate) async fn instant_deriv_returns_gauge_slope_per_second() {
    let store = three_point_gauge_store("temperature_celsius");
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "deriv(temperature_celsius[2m])", 120_000).await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        2.0 / 60.0,
    );
}
