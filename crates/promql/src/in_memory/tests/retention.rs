use super::*;

mod prune_counts_partial_histogram_and_exemplar_retention {
    use super::*;

    #[tokio::test]
    pub(crate) async fn prune_counts_partial_histogram_and_exemplar_retention() {
        let mut store = InMemoryMetricStore::with_retention(secs(1));
        let live = lbls(&[
            crate::test_support::TestLabelPair::new("__name__", "latency_seconds"),
            crate::test_support::TestLabelPair::new("job", "api"),
        ]);
        let stale = lbls(&[
            crate::test_support::TestLabelPair::new("__name__", "latency_seconds"),
            crate::test_support::TestLabelPair::new("job", "old"),
        ]);
        store.push_float("t", live.clone(), 8_999, 1.0);
        store.push_float("t", live.clone(), 9_000, 2.0);
        store.push_float("t", stale.clone(), 1_000, 3.0);
        store.push_histogram("t", live.clone(), 8_999, count_two_sum_three_histogram());
        store.push_histogram("t", live.clone(), 9_000, count_two_sum_three_histogram());
        store.push_exemplar(
            "t",
            live.clone(),
            lbls(&[crate::test_support::TestLabelPair::new("trace_id", "old")]),
            8_999,
            1.0,
        );
        store.push_exemplar(
            "t",
            live.clone(),
            lbls(&[crate::test_support::TestLabelPair::new("trace_id", "new")]),
            9_000,
            2.0,
        );

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
            .unwrap()
            .exemplars;
        check!(exemplars.len() == 1);
        check!(
            exemplars[0].labels
                == lbls(&[crate::test_support::TestLabelPair::new("trace_id", "new")])
        );
        let stale_matchers = [LabelMatcher::new("job", MatchOp::Eq, "old")];
        check!(
            store
                .series("t", &stale_matchers, i64::MIN, i64::MAX)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

mod prune_drops_old_samples {
    use super::*;

    #[tokio::test]
    pub(crate) async fn prune_drops_old_samples() {
        let mut store = InMemoryMetricStore::with_retention(secs(1));
        let series = up_series();
        // ts 100 and 500 are old; ts 9_500 and 9_900 are within the window.
        store.push_float("t", series.clone(), 100, 1.0);
        store.push_float("t", series.clone(), 500, 2.0);
        store.push_float("t", series.clone(), 9_500, 3.0);
        store.push_float("t", series.clone(), 9_900, 4.0);
        // A histogram sample that is also old.
        store.push_histogram("t", series.clone(), 200, count_two_sum_three_histogram());

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
}

mod prune_removes_emptied_series_from_index {
    use super::*;

    #[tokio::test]
    pub(crate) async fn prune_removes_emptied_series_from_index() {
        let mut store = InMemoryMetricStore::with_retention(secs(1));
        let stale = lbls(&[
            crate::test_support::TestLabelPair::new("__name__", "up"),
            crate::test_support::TestLabelPair::new("job", "old"),
        ]);
        let fresh = lbls(&[
            crate::test_support::TestLabelPair::new("__name__", "up"),
            crate::test_support::TestLabelPair::new("job", "new"),
        ]);
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
}

mod prune_skips_unexpired_snapshots_and_handles_late_samples {
    use std::sync::Arc;

    use super::*;

    #[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct TimestampLedgerEntry {
        tenant: String,
        kind: SampleKind,
        timestamp_ms: i64,
    }

    #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
    enum SampleKind {
        Float,
        Histogram,
        Exemplar,
    }

    #[test]
    fn prune_skips_unexpired_snapshots_and_handles_late_samples() {
        let labels = lbls(&[crate::test_support::TestLabelPair::new("__name__", "up")]);
        let mut store = InMemoryMetricStore::with_retention(secs(1));
        for timestamp in 1_000..1_000 + i64::try_from(ROW_CHUNK_LEN * 2).unwrap() {
            store.push_float("t", labels.clone(), timestamp, 1.0);
        }
        let head = WalHead::from_store(store);
        let snapshot = head.snapshot();
        // The oldest row lies exactly on the inclusive retention boundary.
        assert2::assert!(head.prune(2_000) == PruneStats::default());
        assert2::assert!(Arc::ptr_eq(&snapshot, &head.snapshot()));
        assert2::assert!(head.prune(2_001).samples_dropped == 1);
        assert2::assert!(!Arc::ptr_eq(&snapshot, &head.snapshot()));
        assert2::assert!(snapshot.floats["t"].len() == ROW_CHUNK_LEN * 2);

        // A late record must invalidate the shortcut immediately.
        head.update(|store| store.push_float("t", labels, -1_000, 2.0));
        assert2::assert!(head.prune(2_001).samples_dropped == 1);
        let after = head.snapshot();
        assert2::assert!(head.prune(2_001) == PruneStats::default());
        assert2::assert!(Arc::ptr_eq(&after, &head.snapshot()));
    }

    #[test]
    fn retention_matches_a_timestamp_ledger_across_sample_kinds_and_tenants() {
        let labels = lbls(&[crate::test_support::TestLabelPair::new("__name__", "up")]);
        let mut store = InMemoryMetricStore::with_retention(secs(2));
        let mut ledger = Vec::new();
        // Independent row ledger: no cached minimum or chunked storage.
        let push = |store: &mut InMemoryMetricStore,
                    ledger: &mut Vec<TimestampLedgerEntry>,
                    tenant: &'static str,
                    kind,
                    timestamp| {
            match kind {
                0 => store.push_float(tenant, labels.clone(), timestamp, 1.0),
                1 => store.push_histogram(
                    tenant,
                    labels.clone(),
                    timestamp,
                    count_two_sum_three_histogram(),
                ),
                _ => store.push_exemplar(tenant, labels.clone(), Labels::new(), timestamp, 1.0),
            }
            let kind = match kind {
                0 => SampleKind::Float,
                1 => SampleKind::Histogram,
                _ => SampleKind::Exemplar,
            };
            ledger.push(TimestampLedgerEntry {
                tenant: tenant.to_owned(),
                kind,
                timestamp_ms: timestamp,
            });
        };
        let verify = |store: &mut InMemoryMetricStore,
                      ledger: &mut Vec<TimestampLedgerEntry>,
                      now_ms: i64,
                      retention_ms: i64| {
            let before = ledger.len();
            ledger.retain(|entry| entry.timestamp_ms >= now_ms.saturating_sub(retention_ms));
            assert2::assert!(store.prune(now_ms).samples_dropped == before - ledger.len());
            let mut actual = Vec::new();
            for tenant in store.floats.keys() {
                let rows = &store.floats[tenant];
                actual.extend(rows.iter().map(|row| TimestampLedgerEntry {
                    tenant: tenant.clone(),
                    kind: SampleKind::Float,
                    timestamp_ms: row.ts_ms,
                }));
            }
            for tenant in store.hists.keys() {
                let rows = &store.hists[tenant];
                actual.extend(rows.iter().map(|row| TimestampLedgerEntry {
                    tenant: tenant.clone(),
                    kind: SampleKind::Histogram,
                    timestamp_ms: row.ts_ms,
                }));
            }
            for tenant in store.exemplars.keys() {
                let rows = &store.exemplars[tenant];
                actual.extend(rows.iter().map(|row| TimestampLedgerEntry {
                    tenant: tenant.clone(),
                    kind: SampleKind::Exemplar,
                    timestamp_ms: row.ts_ms,
                }));
            }
            actual.sort_unstable();
            let mut expected = ledger.clone();
            expected.sort_unstable();
            assert2::assert!(actual == expected);
        };

        push(&mut store, &mut ledger, "a", 0, 1_000);
        push(&mut store, &mut ledger, "b", 1, 2_000);
        push(&mut store, &mut ledger, "a", 2, 3_000);
        verify(&mut store, &mut ledger, 3_000, 2_000);
        verify(&mut store, &mut ledger, 3_001, 2_000);
        for kind in 0..3 {
            push(&mut store, &mut ledger, "a", kind, 500);
            verify(&mut store, &mut ledger, 3_001, 2_000);
        }
        store.set_retention(secs(5));
        push(&mut store, &mut ledger, "c", 0, 100);
        store.delete_tenant("c");
        ledger.retain(|entry| entry.tenant != "c");
        verify(&mut store, &mut ledger, 8_000, 5_000);
        store.set_retention(secs(1));
        verify(&mut store, &mut ledger, 8_000, 1_000);
        for kind in 0..3 {
            push(&mut store, &mut ledger, "b", kind, i64::MIN);
        }
        verify(&mut store, &mut ledger, i64::MIN, 1_000);
        verify(&mut store, &mut ledger, i64::MIN + 1_001, 1_000);
        verify(&mut store, &mut ledger, i64::MAX, 1_000);
    }
}

mod pruning_a_chunked_head_leaves_open_snapshots_intact {
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
        let series = up_series();
        for row in 0..rows {
            let ts_ms = i64::try_from(row).expect("a row index fits an i64") + 1;
            store.push_float("t", series.clone(), ts_ms, 1.0);
        }
        let head = WalHead::from_store(store);

        let total = i64::try_from(rows).expect("a row count fits an i64");
        let before = head.snapshot();
        assert2::assert!(count_up_float_rows(&before).await == total);

        // Retention is a second and the newest sample sits at `total`, so every
        // sample below `total - 1_000` goes and the thousand-and-one above it stay.
        let stats = head.prune(total);
        assert2::assert!(stats.samples_dropped == usize::try_from(total - 1_001).unwrap());
        assert2::assert!(stats.series_dropped == 0);

        assert2::assert!(count_up_float_rows(&head.snapshot()).await == 1_001);
        // The snapshot predates the prune and is unmoved by it.
        assert2::assert!(count_up_float_rows(&before).await == total);
    }
}
