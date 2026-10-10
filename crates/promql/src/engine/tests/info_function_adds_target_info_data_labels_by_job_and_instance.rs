use super::*;

#[tokio::test]
pub(crate) async fn info_function_adds_target_info_data_labels_by_job_and_instance() {
    let engine = target_info_engine();
    let samples = instant_vector(&engine, "info(http_requests_total)", 10_000).await;
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__") == Some("http_requests_total"));
    check!(samples[0].labels.get("job") == Some("api"));
    check!(samples[0].labels.get("instance") == Some("a"));
    check!(samples[0].labels.get("region") == Some("east"));
    check!(samples[0].labels.get("cluster") == Some("prod"));
    check!(approx_eq(float_value(&samples[0].value), 7.0));
}
