use super::*;

#[tokio::test]
pub(crate) async fn instant_selector_honors_label_matchers_and_tenant() {
    let engine = FloatStore::default()
        .sample(
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]),
            1.0,
        )
        .sample(
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "web"),
            ]),
            0.0,
        )
        .tenant_sample(
            "tenant-b",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]),
            9.0,
        )
        .engine();
    let samples = instant_vector(&engine, r#"up{job=~"a.*"}"#, 10_000).await;
    check!(samples.len() == 1);
    check!(samples[0].labels.get("job") == Some("api"));
    check!(approx_eq(float_value(&samples[0].value), 1.0));
}
