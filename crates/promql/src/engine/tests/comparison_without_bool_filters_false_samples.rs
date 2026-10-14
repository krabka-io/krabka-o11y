use super::*;

#[tokio::test]
pub(crate) async fn comparison_without_bool_filters_false_samples() {
    let engine = comparison_test_engine();
    let samples = instant_vector(&engine, "a > 100", 10_000).await;
    assert2::assert!(samples.is_empty());
}
