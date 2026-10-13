use super::*;

#[tokio::test]
pub(crate) async fn vector_scalar_arithmetic_preserves_labels_and_drops_metric_name() {
    let engine = FloatStore::default()
        .sample(
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]),
            1.0,
        )
        .engine();
    let samples = instant_vector(&engine, "up * 2", 10_000).await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        2.0,
    );
}
