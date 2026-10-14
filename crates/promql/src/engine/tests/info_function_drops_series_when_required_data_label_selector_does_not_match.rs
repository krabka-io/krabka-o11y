use super::*;

#[tokio::test]
pub(crate) async fn info_function_drops_series_when_required_data_label_selector_does_not_match() {
    let engine = info_metrics_engine(vec![super::target_info_engine::info_series_labels(
        "target_info",
        &[("region", "east")],
    )]);
    let samples = instant_vector(
        &engine,
        r#"info(http_requests_total, {region="west"})"#,
        10_000,
    )
    .await;
    assert2::assert!(samples.is_empty());
}
