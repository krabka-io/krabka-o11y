#[cfg(feature = "experimental-functions")]
use super::*;

#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn instant_double_exponential_smoothing_smooths_gauge_series() {
    let store = SeriesFixture::new(labels(&[
        crate::test_support::TestLabelPair::new("__name__", "queue_depth"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]))
    .at(0_i64, 3.0)
    .at(60_000, 6.0)
    .at(120_000, 12.0)
    .at(180_000, 21.0)
    .store();

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(
        &engine,
        "double_exponential_smoothing(queue_depth[4m], 0.5, 0.5)",
        180_000,
    )
    .await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        17.625,
    );
}
