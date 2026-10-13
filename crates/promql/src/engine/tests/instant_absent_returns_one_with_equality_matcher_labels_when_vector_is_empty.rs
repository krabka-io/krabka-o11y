use super::*;

#[tokio::test]
pub(crate) async fn instant_absent_returns_one_with_equality_matcher_labels_when_vector_is_empty() {
    let engine = FloatStore::default().up_sample("api", "a", 1.0).engine();
    let result = engine
        .query_instant(
            &tenant_id("tenant-a"),
            r#"absent(up{job="worker",instance=~".*"})"#,
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("worker"));
    check!(samples[0].labels.get("instance").is_none());
    check!(approx_eq(float_value(&samples[0].value), 1.0));
}
