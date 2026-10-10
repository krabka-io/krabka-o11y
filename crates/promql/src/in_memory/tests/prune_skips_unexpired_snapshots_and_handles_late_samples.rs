use std::sync::Arc;

use super::*;

#[test]
fn prune_skips_unexpired_snapshots_and_handles_late_samples() {
    let labels = lbls(&[("__name__", "up")]);
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
    let labels = lbls(&[("__name__", "up")]);
    let mut store = InMemoryMetricStore::with_retention(secs(2));
    let mut ledger = Vec::new();
    // Independent row ledger: no cached minimum or chunked storage.
    let push = |store: &mut InMemoryMetricStore,
                ledger: &mut Vec<(&str, u8, i64)>,
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
        ledger.push((tenant, kind, timestamp));
    };
    let verify = |store: &mut InMemoryMetricStore,
                  ledger: &mut Vec<(&str, u8, i64)>,
                  now_ms: i64,
                  retention_ms: i64| {
        let before = ledger.len();
        ledger.retain(|(_, _, timestamp)| *timestamp >= now_ms.saturating_sub(retention_ms));
        assert2::assert!(store.prune(now_ms).samples_dropped == before - ledger.len());
        let mut actual = Vec::new();
        for (tenant, rows) in &store.floats {
            actual.extend(rows.iter().map(|row| (tenant.as_str(), 0, row.ts_ms)));
        }
        for (tenant, rows) in &store.hists {
            actual.extend(rows.iter().map(|row| (tenant.as_str(), 1, row.ts_ms)));
        }
        for (tenant, rows) in &store.exemplars {
            actual.extend(rows.iter().map(|row| (tenant.as_str(), 2, row.ts_ms)));
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
    ledger.retain(|(tenant, _, _)| *tenant != "c");
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
