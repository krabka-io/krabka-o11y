use super::*;

#[tokio::test]
pub(crate) async fn instant_absent_over_time_returns_one_when_range_is_empty() {
    let engine = FloatStore::default()
        .sample(labels(&[("__name__", "up"), ("job", "api")]), 1.0)
        .engine();
    let samples = instant_vector(&engine, r#"absent_over_time(up{job="api"}[1m])"#, 120_000).await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        1.0,
    );
}
