use super::*;

#[tokio::test]
pub(crate) async fn info_function_merges_data_labels_from_multiple_info_metrics() {
    let engine = info_metrics_engine(vec![
        labels(&[
            ("__name__", "target_info"),
            ("job", "api"),
            ("instance", "a"),
            ("cluster", "prod"),
        ]),
        labels(&[
            ("__name__", "build_info"),
            ("job", "api"),
            ("instance", "a"),
            ("version", "1.2.3"),
        ]),
    ]);
    let samples = instant_vector(
        &engine,
        r#"info(http_requests_total, {__name__=~".+_info"})"#,
        10_000,
    )
    .await;
    assert2::assert!(samples.len() == 1);
    assert2::assert!(samples[0].labels.get("cluster") == Some("prod"));
    assert2::assert!(samples[0].labels.get("version") == Some("1.2.3"));
}
