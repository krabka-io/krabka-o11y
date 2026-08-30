use super::*;

#[tokio::test]
pub(crate) async fn prune_drops_old_samples() {
    let mut store = InMemoryMetricStore::with_retention(secs(1));
    let series = lbls(&[("__name__", "up"), ("job", "api")]);
    // ts 100 and 500 are old; ts 9_500 and 9_900 are within the window.
    store.push_float("t", series.clone(), 100, 1.0);
    store.push_float("t", series.clone(), 500, 2.0);
    store.push_float("t", series.clone(), 9_500, 3.0);
    store.push_float("t", series.clone(), 9_900, 4.0);
    // A histogram sample that is also old.
    store.push_histogram("t", series.clone(), 200, native_histogram());

    // now = 10_000, retention = 1_000 -> cutoff = 9_000; ts < 9_000 dropped.
    let stats = store.prune(10_000);
    assert2::assert!(stats.samples_dropped == 3);
    assert2::assert!(stats.series_dropped == 0);

    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let remaining = store
        .scan("t", &matchers, i64::MIN, i64::MAX)
        .await
        .unwrap();
    let table = remaining.float_table.clone().unwrap();
    let df = remaining
        .ctx
        .sql(&format!("SELECT count(*) AS c FROM {table}"))
        .await
        .unwrap();
    let output = df.collect().await.unwrap();
    let count = output[0].column(0).as_primitive::<Int64Type>().value(0);
    assert2::assert!(count == 2);
    // The old histogram sample is gone.
    assert2::assert!(remaining.histogram_table.is_none());
}
