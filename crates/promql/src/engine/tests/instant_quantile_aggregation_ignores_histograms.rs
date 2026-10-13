use super::*;

#[tokio::test]
pub(crate) async fn instant_quantile_aggregation_ignores_histograms() {
    let store = mixed_metric_store(
        "latency_seconds",
        &[
            MixedMetricFloatSample::builder()
                .instance("a")
                .value(2.0)
                .build(),
            MixedMetricFloatSample::builder()
                .instance("b")
                .value(6.0)
                .build(),
        ],
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "quantile by (job) (0.5, latency_seconds)", 10_000).await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        4.0,
    );
}
