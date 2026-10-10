use super::*;

#[tokio::test]
pub(crate) async fn instant_sum_by_groups_by_exact_labels_and_drops_metric_name() {
    let engine = FloatStore::default()
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "a")]),
            1.0,
        )
        .sample(
            labels(&[("__name__", "up"), ("job", "api"), ("instance", "b")]),
            2.0,
        )
        .sample(
            labels(&[("__name__", "up"), ("job", "web"), ("instance", "c")]),
            4.0,
        )
        .engine();
    let samples = instant_vector(&engine, "sum by (job) (up)", 10_000).await;
    assert2::assert!(samples.len() == 2);
    let api = samples
        .iter()
        .find(|sample| sample.labels.get("job") == Some("api"))
        .expect("api group");
    assert2::assert!(api.labels.get("__name__") == None);
    assert2::assert!(api.labels.get("instance") == None);
    assert2::assert!(approx_eq(float_value(&api.value), 3.0));
    let web = samples
        .iter()
        .find(|sample| sample.labels.get("job") == Some("web"))
        .expect("web group");
    assert2::assert!(approx_eq(float_value(&web.value), 4.0));
}
