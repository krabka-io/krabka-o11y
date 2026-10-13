use super::*;

#[tokio::test]
pub(crate) async fn info_function_drops_series_when_required_data_label_selector_does_not_match() {
    let engine = info_metrics_engine(vec![labels(&[
        crate::test_support::TestLabelPair::new("__name__", "target_info"),
        crate::test_support::TestLabelPair::new("job", "api"),
        crate::test_support::TestLabelPair::new("instance", "a"),
        crate::test_support::TestLabelPair::new("region", "east"),
    ])]);
    let samples = instant_vector(
        &engine,
        r#"info(http_requests_total, {region="west"})"#,
        10_000,
    )
    .await;
    assert2::assert!(samples.is_empty());
}
