use super::*;

/// Rows keep their identity across the chunk boundary that copy-on-write
/// sharing introduces, and a prune rewrites a chunk by replacing the pointer
/// rather than editing it in place. The second half is the invariant the
/// sharing rests on: a snapshot taken before a prune still answers with every
/// row it captured, including the ones the prune dropped from the live head.
#[tokio::test]
pub(crate) async fn pruning_a_chunked_head_leaves_open_snapshots_intact() {
    // Two full chunks and part of a third, so the head holds sealed chunks and
    // an open one at once and the prune has to cross both kinds.
    let rows = ROW_CHUNK_LEN * 2 + 7;
    let mut store = InMemoryMetricStore::with_retention(secs(1));
    let series = lbls(&[("__name__", "up"), ("job", "api")]);
    for row in 0..rows {
        let ts_ms = i64::try_from(row).expect("a row index fits an i64") + 1;
        store.push_float("t", series.clone(), ts_ms, 1.0);
    }
    let head = WalHead::from_store(store);

    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let count = async |store: &InMemoryMetricStore| {
        let scan = store
            .scan("t", &matchers, i64::MIN, i64::MAX)
            .await
            .unwrap();
        let table = scan.float_table.clone().unwrap();
        count_rows(&scan, &table).await
    };

    let total = i64::try_from(rows).expect("a row count fits an i64");
    let before = head.snapshot();
    assert2::assert!(count(&before).await == total);

    // Retention is a second and the newest sample sits at `total`, so every
    // sample below `total - 1_000` goes and the thousand-and-one above it stay.
    let stats = head.prune(total);
    assert2::assert!(stats.samples_dropped == usize::try_from(total - 1_001).unwrap());
    assert2::assert!(stats.series_dropped == 0);

    assert2::assert!(count(&head.snapshot()).await == 1_001);
    // The snapshot predates the prune and is unmoved by it.
    assert2::assert!(count(&before).await == total);
}
