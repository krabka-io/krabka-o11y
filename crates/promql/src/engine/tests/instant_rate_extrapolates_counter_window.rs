use super::*;

#[tokio::test]
pub(crate) async fn instant_rate_extrapolates_counter_window() {
    let engine = requests_counter_engine(4);
    let samples = instant_vector(&engine, "rate(http_requests_total[5m])", 300_000).await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        5.0 / 300.0,
    );
}
