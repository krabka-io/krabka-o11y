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
    assert_label_transform(
        &engine,
        r#"label_join(up, "target", "/", "job", "instance")"#,
        "target",
        "api/a",
        "zone",
        "us-east-1a",
    )
    .await;
}
