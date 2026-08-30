use super::*;

#[tokio::test]
pub(crate) async fn prune_removes_emptied_series_from_index() {
    let mut store = InMemoryMetricStore::with_retention(secs(1));
    let stale = lbls(&[("__name__", "up"), ("job", "old")]);
    let fresh = lbls(&[("__name__", "up"), ("job", "new")]);
    store.push_float("t", stale.clone(), 100, 1.0);
    store.push_float("t", fresh.clone(), 9_900, 2.0);

    let stats = store.prune(10_000);
    assert2::assert!(stats.samples_dropped == 1);
    assert2::assert!(stats.series_dropped == 1);

    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let series = store
        .series("t", &matchers, i64::MIN, i64::MAX)
        .await
        .unwrap();
    assert2::assert!(series == vec![fresh.clone()]);

    // The emptied series' label value no longer appears on the label surface.
    let jobs = store
        .label_values("t", "job", &matchers, i64::MIN, i64::MAX)
        .await
        .unwrap();
    assert2::assert!(jobs == vec!["new".to_string()]);
}
