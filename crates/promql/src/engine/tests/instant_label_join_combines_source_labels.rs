use super::*;

#[tokio::test]
pub(crate) async fn instant_label_join_combines_source_labels() {
    let engine = FloatStore::default()
        .named_sample(
            "up",
            &[
                crate::test_support::TestLabelPair::new("job", "api"),
                crate::test_support::TestLabelPair::new("instance", "a"),
                crate::test_support::TestLabelPair::new("zone", "us-east-1a"),
            ],
            1.0,
        )
        .engine();
    let result = engine
        .query_instant(
            &tenant_id("tenant-a"),
            r#"label_join(up, "target", "/", "job", "instance")"#,
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(samples[0].labels.get("target") == Some("api/a"));
    assert2::assert!(samples[0].labels.get("zone") == Some("us-east-1a"));
    assert2::assert!(approx_eq(float_value(&samples[0].value), 1.0));
}
