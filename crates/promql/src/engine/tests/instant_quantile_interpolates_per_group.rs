use super::*;

#[tokio::test]
pub(crate) async fn instant_quantile_interpolates_per_group() {
    let engine = api_latency_engine(&[1.0, 2.0, 4.0, 8.0]);
    let samples = instant_vector(&engine, "quantile by (job) (0.5, latency_seconds)", 10_000).await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        3.0,
    );
}
