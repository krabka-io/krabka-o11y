use std::{collections::BTreeMap, sync::Arc};

use assert2::assert;

use super::*;

#[test]
fn wal_batch_preserves_rows_snapshots_and_watermarks() {
    let head = WalHead::new();
    let before = head.snapshot();
    let first = WalRecord {
        tenant: "t".into(),
        labels: [("b", "old"), ("a", "=x\n"), ("b", "last")]
            .map(|(k, v)| (k.into(), v.into()))
            .into(),
        payload: SamplePayload::Float {
            timestamp_ms: 100,
            value: 1.0,
            start_timestamp_ms: Some(50),
        },
        exemplars: Vec::new(),
    };
    let labels = lbls(&[("a", "=x\n"), ("b", "last")]);
    let other = lbls(&[("é", "🦀\0"), ("", ""), ("x", "y=z\n")]);
    let record = |tenant: &str, labels: &Labels, payload, exemplars| WalRecord {
        tenant: tenant.into(),
        labels: labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        payload,
        exemplars,
    };
    let records = [
        (first, PartitionIndex(0), Offset(7)),
        (
            record(
                "t",
                &labels,
                SamplePayload::Hist {
                    timestamp_ms: 200,
                    hist: count_two_sum_three_histogram(),
                },
                Vec::new(),
            ),
            PartitionIndex(0),
            Offset(8),
        ),
        (
            record(
                "other",
                &other,
                SamplePayload::Float {
                    timestamp_ms: 150,
                    value: 3.0,
                    start_timestamp_ms: None,
                },
                Vec::new(),
            ),
            PartitionIndex(1),
            Offset(20),
        ),
        (
            record(
                "t",
                &Labels::new(),
                SamplePayload::Metadata {
                    metric_family_name: "m".into(),
                    metric_type: "gauge".into(),
                    help: "help".into(),
                    unit: "seconds".into(),
                },
                Vec::new(),
            ),
            PartitionIndex(1),
            Offset(21),
        ),
        (
            record(
                "t",
                &labels,
                SamplePayload::Exemplars,
                vec![WalExemplar {
                    labels: vec![("trace_id".into(), "abc".into())],
                    timestamp_ms: 300,
                    value: 4.0,
                }],
            ),
            PartitionIndex(0),
            Offset(9),
        ),
    ];
    head.apply_wal_records_at(
        records
            .iter()
            .map(|(record, partition, offset)| (record, *partition, *offset)),
    );
    let after = head.snapshot();
    let mut floats = after
        .floats
        .iter()
        .flat_map(|(tenant, rows)| {
            rows.iter().map(move |r| {
                (
                    tenant.as_str(),
                    r.fp,
                    r.labels.as_ref().clone(),
                    r.ts_ms,
                    r.value.to_bits(),
                    r.start_timestamp_ms,
                )
            })
        })
        .collect::<Vec<_>>();
    floats.sort_by(|left, right| left.0.cmp(right.0));
    assert2::assert!(
        floats
            == [
                (
                    "other",
                    0x56a3_88ef_237b_8646,
                    other,
                    150,
                    3.0_f64.to_bits(),
                    None
                ),
                (
                    "t",
                    0xd1e5_33a9_4f60_f896,
                    labels.clone(),
                    100,
                    1.0_f64.to_bits(),
                    Some(50)
                ),
            ]
    );
    let first = after.floats["t"].iter().next().unwrap();
    assert2::assert!(after.hists.len() == 1 && after.hists["t"].len() == 1);
    let hist = after.hists["t"].iter().next().unwrap();
    assert2::assert!(
        (
            hist.fp,
            hist.labels.as_ref(),
            hist.ts_ms,
            hist.hist.as_ref()
        ) == (
            0xd1e5_33a9_4f60_f896,
            &labels,
            200,
            &count_two_sum_three_histogram()
        )
    );
    assert2::assert!(Arc::ptr_eq(&first.labels, &hist.labels));
    assert2::assert!(after.exemplars.len() == 1 && after.exemplars["t"].len() == 1);
    let exemplar = after.exemplars["t"].iter().next().unwrap();
    assert2::assert!(
        (
            exemplar.series_labels.as_ref(),
            exemplar.labels.as_ref(),
            exemplar.ts_ms,
            exemplar.value
        ) == (&labels, &lbls(&[("trace_id", "abc")]), 300, 4.0)
    );
    assert2::assert!(Arc::ptr_eq(&first.labels, &exemplar.series_labels));
    assert2::assert!(after.metadata.len() == 1 && after.metadata["t"].len() == 1);
    let m = after.metadata["t"].iter().next().unwrap();
    assert2::assert!(
        (&*m.metric_family_name, &*m.metric_type, &*m.help, &*m.unit)
            == ("m", "gauge", "help", "seconds")
    );
    assert2::assert!(
        before.floats.is_empty()
            && before.hists.is_empty()
            && before.metadata.is_empty()
            && before.exemplars.is_empty()
    );
    assert2::assert!(
        after
            .watermarks
            .iter()
            .map(|(p, w)| (*p, w.low_water_offset, w.high_water_offset))
            .collect::<Vec<_>>()
            == vec![
                (PartitionIndex(0), Offset(7), Offset(9)),
                (PartitionIndex(1), Offset(20), Offset(21))
            ]
    );
    let weak_labels = Arc::downgrade(&first.labels);
    head.delete_tenant("t");
    assert2::assert!(!head.snapshot().floats.contains_key("t"));
    assert2::assert!(weak_labels.upgrade().is_some());
    drop(after);
    assert2::assert!(weak_labels.upgrade().is_none());
}

#[test]
fn wal_batch_panic_does_not_publish_a_prefix_or_poison_later_writes() {
    let head = WalHead::new();
    let labels = lbls(&[("app", "api")]);
    let first = float_record("t", &labels, 100, 1.0);
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        head.apply_wal_records_at(
            std::iter::once((&first, PartitionIndex(0), Offset(7)))
                .chain(std::iter::from_fn(|| panic!("failed after first record"))),
        );
    }));
    assert2::assert!(panicked.is_err());
    assert2::assert!(head.snapshot().floats.is_empty());
    assert2::assert!(head.high_water_offset(PartitionIndex(0)).is_none());
    let next = float_record("t", &labels, 200, 2.0);
    head.apply_wal_records_at([(&next, PartitionIndex(0), Offset(8))]);
    let snapshot = head.snapshot();
    assert2::assert!(
        snapshot.floats["t"]
            .iter()
            .map(|r| (r.labels.as_ref().clone(), r.ts_ms, r.value))
            .collect::<Vec<_>>()
            == vec![(labels, 200, 2.0)]
    );
    assert2::assert!(head.low_water_offset(PartitionIndex(0)) == Some(Offset(8)));
    assert2::assert!(head.high_water_offset(PartitionIndex(0)) == Some(Offset(8)));
}

#[test]
fn latest_summary_follows_atomic_wal_batches_prune_delete_and_rollback() {
    let head = WalHead::with_retention(secs(1));
    let before = head.snapshot();
    let labels = lbls(&[("app", "api")]);
    let retired = lbls(&[("app", "retired")]);
    let record = |labels: &Labels, stamp, value, start| WalRecord {
        tenant: "t".into(),
        labels: labels
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        payload: SamplePayload::Float {
            timestamp_ms: stamp,
            value,
            start_timestamp_ms: start,
        },
        exemplars: Vec::new(),
    };
    let records = [
        record(&labels, 100, 1.0, Some(0)),
        record(&labels, 1_100, 7.0, Some(0)),
        record(&labels, 1_100, 99.0, None),
        record(&labels, 1_000, 4.0, None),
        record(&retired, 100, 10.0, None),
    ];
    head.apply_wal_records_at(
        records
            .iter()
            .zip([7, 8, 9, 10, 11])
            .map(|(record, offset)| (record, PartitionIndex(0), Offset(offset))),
    );
    let batch = head.snapshot();
    let live_labels = Arc::downgrade(&batch.floats["t"].iter().next().unwrap().labels);
    let retired_labels = Arc::downgrade(&batch.floats["t"].iter().last().unwrap().labels);
    let ledger = |snapshot: &InMemoryMetricStore| {
        super::interned_series_labels::summary_ledger(snapshot, "t", &[], 0, 0, 3_000)
    };
    let retained = BTreeMap::from([
        (
            labels.fingerprint(),
            (Arc::new(labels.clone()), 1_100, 7.0_f64.to_bits(), Some(0)),
        ),
        (
            retired.fingerprint(),
            (Arc::new(retired), 100, 10.0_f64.to_bits(), None),
        ),
    ]);
    let surviving = BTreeMap::from([(
        labels.fingerprint(),
        (Arc::new(labels.clone()), 1_100, 7.0_f64.to_bits(), Some(0)),
    )]);
    assert!(ledger(&before).is_none() && before.watermarks().is_empty());
    assert!(ledger(&batch) == Some((retained.clone(), 5)));
    assert!(head.prune(2_000).samples_dropped == 2);
    let pruned = head.snapshot();
    assert!(ledger(&pruned) == Some((surviving.clone(), 3)));
    assert!(ledger(&batch) == Some((retained, 5)));
    assert!(
        head.low_water_offset(PartitionIndex(0)) == Some(Offset(7))
            && head.high_water_offset(PartitionIndex(0)) == Some(Offset(11))
    );
    assert!(pruned.watermarks() == batch.watermarks());
    assert!(retired_labels.upgrade().is_some());
    drop(batch);
    assert!(retired_labels.upgrade().is_none());

    let provisional = record(&labels, 2_100, 8.0, None);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        head.apply_wal_records_at(
            std::iter::once((&provisional, PartitionIndex(0), Offset(12))).chain(
                std::iter::from_fn(|| panic!("failed after provisional latest")),
            ),
        );
    }));
    assert!(panic.is_err());
    assert!(Arc::ptr_eq(&pruned, &head.snapshot()));
    assert!(ledger(&head.snapshot()) == Some((surviving.clone(), 3)));
    assert!(head.high_water_offset(PartitionIndex(0)) == Some(Offset(11)));
    let next = record(&labels, 2_200, 9.0, None);
    head.apply_wal_records_at([(&next, PartitionIndex(0), Offset(13))]);
    let published = head.snapshot();
    assert!(
        ledger(&published)
            == Some((
                BTreeMap::from([(
                    labels.fingerprint(),
                    (Arc::new(labels), 2_200, 9.0_f64.to_bits(), None)
                ),]),
                4
            ))
    );
    assert!(ledger(&pruned) == Some((surviving, 3)));
    assert!(
        head.low_water_offset(PartitionIndex(0)) == Some(Offset(7))
            && head.high_water_offset(PartitionIndex(0)) == Some(Offset(13))
    );
    head.delete_tenant("t");
    let deleted = head.snapshot();
    assert!(
        ledger(&deleted).is_none() && deleted.floats.is_empty() && deleted.series_labels.is_empty()
    );
    assert!(deleted.watermarks() == published.watermarks());
    assert!(live_labels.upgrade().is_some());
    drop(pruned);
    drop(published);
    assert!(live_labels.upgrade().is_none());
}
