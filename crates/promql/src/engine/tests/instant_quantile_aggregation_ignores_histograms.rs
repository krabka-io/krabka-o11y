use super::*;

#[derive(bon::Builder)]
struct LatencySample {
    instance: &'static str,
    value: f64,
}

#[tokio::test]
pub(crate) async fn instant_quantile_aggregation_ignores_histograms() {
    let mut store = InMemoryMetricStore::new();
    for sample in [
        LatencySample::builder().instance("a").value(2.0).build(),
        LatencySample::builder().instance("b").value(6.0).build(),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "latency_seconds"),
                crate::test_support::TestLabelPair::new("job", "api"),
                crate::test_support::TestLabelPair::new("instance", sample.instance),
            ]),
            10_000,
            sample.value,
        );
    }
    store.push_histogram(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "latency_seconds"),
            crate::test_support::TestLabelPair::new("job", "api"),
            crate::test_support::TestLabelPair::new("instance", "hist"),
        ]),
        10_000,
        native_histogram(4.0, 10.0),
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
