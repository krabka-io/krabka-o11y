use super::*;

#[tokio::test]
pub(crate) async fn instant_selector_honors_label_matchers_and_tenant() {
    let engine = FloatStore::default()
        .sample(labels(&[("__name__", "up"), ("job", "api")]), 1.0)
        .sample(labels(&[("__name__", "up"), ("job", "web")]), 0.0)
        .tenant_sample(
            "tenant-b",
            labels(&[("__name__", "up"), ("job", "api")]),
            9.0,
        )
        .engine();
    let samples = instant_vector(&engine, r#"up{job=~"a.*"}"#, 10_000).await;
    check!(samples.len() == 1);
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 1.0));
}
