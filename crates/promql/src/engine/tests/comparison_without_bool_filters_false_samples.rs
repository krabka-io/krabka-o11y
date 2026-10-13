use super::*;

#[tokio::test]
pub(crate) async fn comparison_without_bool_filters_false_samples() {
    let engine = FloatStore::default()
        .sample(
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "a"),
                crate::test_support::TestLabelPair::new("x", "1"),
            ]),
            10.0,
        )
        .engine();
    let samples = instant_vector(&engine, "a > 100", 10_000).await;
    assert2::assert!(samples.is_empty());
}
