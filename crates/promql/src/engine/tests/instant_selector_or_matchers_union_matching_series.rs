use super::*;

#[tokio::test]
pub(crate) async fn instant_selector_or_matchers_union_matching_series() {
    let engine = FloatStore::default()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            1.0,
        )
        .sample(
            labels(&[("__name__", "up"), ("job", "web"), ("instance", "b")]),
            2.0,
        )
        .sample(
            labels(&[("__name__", "up"), ("job", "db"), ("instance", "c")]),
            3.0,
        )
        .engine();
    let samples = instant_vector(&engine, r#"up{job="api" or job="web"}"#, 10_000).await;
    assert2::assert!(samples.len() == 2);
    let values_by_job = samples
        .iter()
        .map(|sample| {
            (
                sample.labels.get("job").expect("job label").to_string(),
                float_value(&sample.value),
            )
        })
        .collect::<BTreeMap<_, _>>();
    check!(approx_eq(values_by_job["api"], 1.0));
    check!(approx_eq(values_by_job["web"], 2.0));
    check!(!values_by_job.contains_key("db"));
}
