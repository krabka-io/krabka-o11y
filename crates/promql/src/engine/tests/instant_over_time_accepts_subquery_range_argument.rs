use super::*;

#[tokio::test]
pub(crate) async fn instant_over_time_accepts_subquery_range_argument() {
    let engine = queue_depth_engine();
    let samples = instant_vector(&engine, "avg_over_time((queue_depth * 2)[2m:1m])", 120_000).await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        5.0,
    );
}
