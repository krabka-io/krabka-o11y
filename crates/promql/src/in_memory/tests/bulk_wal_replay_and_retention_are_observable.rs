use super::*;

#[tokio::test]
pub(crate) async fn bulk_wal_replay_and_retention_are_observable() {
    assert2::assert!(DEFAULT_RETENTION == hours(6));

    let records = [
        float_record(
            "tenant-a",
            &lbls(&[("__name__", "up"), ("job", "api")]),
            10_000,
            1.0,
        ),
        float_record(
            "tenant-a",
            &lbls(&[("__name__", "up"), ("job", "worker")]),
            20_000,
            2.0,
        ),
    ];

    let mut store = InMemoryMetricStore::with_retention(secs(5));
    assert2::assert!(store.retention() == secs(5));
    store.set_retention(secs(7));
    assert2::assert!(store.retention() == secs(7));
    store.apply_wal_records(&records);

    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let series = store
        .series("tenant-a", &matchers, 0, 30_000)
        .await
        .unwrap();
    assert2::assert!(series.len() == 2);

    let head = WalHead::with_retention(secs(9));
    assert2::assert!(head.retention() == secs(9));
    head.apply_wal_records(&records);
    let jobs = head
        .label_values("tenant-a", "job", &matchers, 0, 30_000)
        .await
        .unwrap();
    assert2::assert!(jobs == vec!["api".to_string(), "worker".to_string()]);

    let stats = head.prune(20_000);
    assert2::assert!(
        stats
            == PruneStats {
                samples_dropped: 1,
                series_dropped: 1,
            }
    );
    let jobs = head
        .label_values("tenant-a", "job", &matchers, 0, 30_000)
        .await
        .unwrap();
    assert2::assert!(jobs == vec!["worker".to_string()]);
}
