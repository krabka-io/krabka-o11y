use super::*;

#[tokio::test]
pub(crate) async fn info_function_uses_data_label_selector_to_filter_and_copy_labels() {
    let engine = target_info_engine();
    let samples = query_instant_vector(
        &engine,
        "tenant-a",
        r#"info(http_requests_total, {region="east"})"#,
        10_000,
    )
    .await;
    assert2::assert!(samples.len() == 1);
    assert2::assert!(
        samples[0].labels.clone()
            == labels(&[
                crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
                crate::test_support::TestLabelPair::new("job", "api"),
                crate::test_support::TestLabelPair::new("instance", "a"),
                crate::test_support::TestLabelPair::new("region", "east")
            ])
    );
    assert2::assert!(approx_eq(float_value(&samples[0].value), 7.0));
}
