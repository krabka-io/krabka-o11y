use super::*;

#[tokio::test]
pub(crate) async fn instant_selector_or_matchers_union_matching_series() {
    let engine = FloatStore::default()
        .sample(
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "api"),
                crate::test_support::TestLabelPair::new("instance", "a"),
            ]),
            1.0,
        )
        .sample(
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "web"),
                crate::test_support::TestLabelPair::new("instance", "b"),
            ]),
            2.0,
        )
        .sample(
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "db"),
                crate::test_support::TestLabelPair::new("instance", "c"),
            ]),
            3.0,
        )
        .engine();
    let samples = instant_vector(&engine, r#"up{job="api" or job="web"}"#, 10_000).await;
    assert2::assert!(samples.len() == 2);
    let mut values_by_job = BTreeMap::new();
    for sample in &samples {
        values_by_job.insert(
            sample.labels.get("job").expect("job label").to_string(),
            float_value(&sample.value),
        );
    }
    check!(approx_eq(values_by_job["api"], 1.0));
    check!(approx_eq(values_by_job["web"], 2.0));
    check!(!values_by_job.contains_key("db"));
}
