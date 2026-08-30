use super::*;

#[tokio::test]
pub(crate) async fn prune_counts_partial_histogram_and_exemplar_retention() {
    let mut store = InMemoryMetricStore::with_retention(secs(1));
    let live = lbls(&[("__name__", "latency_seconds"), ("job", "api")]);
    let stale = lbls(&[("__name__", "latency_seconds"), ("job", "old")]);
    store.push_float("t", live.clone(), 8_999, 1.0);
    store.push_float("t", live.clone(), 9_000, 2.0);
    store.push_float("t", stale.clone(), 1_000, 3.0);
    store.push_histogram("t", live.clone(), 8_999, native_histogram());
    store.push_histogram("t", live.clone(), 9_000, native_histogram());
    store.push_exemplar("t", live.clone(), lbls(&[("trace_id", "old")]), 8_999, 1.0);
    store.push_exemplar("t", live.clone(), lbls(&[("trace_id", "new")]), 9_000, 2.0);

    let stats = store.prune(10_000);
    assert2::assert!(
        stats
            == PruneStats {
                samples_dropped: 4,
                series_dropped: 1,
            }
    );

    let matchers = [LabelMatcher::new("job", MatchOp::Eq, "api")];
    let result = store
        .scan("t", &matchers, i64::MIN, i64::MAX)
        .await
        .unwrap();
    check!(count_rows(&result, result.float_table.as_ref().unwrap()).await == 1);
    check!(count_rows(&result, result.histogram_table.as_ref().unwrap()).await == 1);
    let exemplars = store
        .exemplars("t", &matchers, i64::MIN, i64::MAX)
        .await
        .unwrap();
    check!(exemplars.len() == 1);
    check!(exemplars[0].labels == lbls(&[("trace_id", "new")]));
    let stale_matchers = [LabelMatcher::new("job", MatchOp::Eq, "old")];
    check!(
        store
            .series("t", &stale_matchers, i64::MIN, i64::MAX)
            .await
            .unwrap()
            .is_empty()
    );
}
