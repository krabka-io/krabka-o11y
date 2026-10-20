use std::{collections::BTreeMap, sync::Arc};

use assert2::assert;

use super::*;

struct PositionedWalRecord {
    record: WalRecord,
    partition: PartitionIndex,
    offset: Offset,
}

#[derive(Debug, PartialEq)]
struct FloatRowSnapshot<'a> {
    tenant: &'a str,
    fingerprint: u64,
    labels: Labels,
    timestamp_ms: i64,
    value_bits: u64,
    start_timestamp_ms: Option<i64>,
}

#[test]
fn wal_batch_preserves_rows_snapshots_and_watermarks() {
    let head = WalHead::new();
    let before = head.snapshot();
    let first = WalRecord {
        tenant: "t".into(),
        labels: vec![
            krabka_metrics::MetricLabelPair::new("b", "old".into()),
            krabka_metrics::MetricLabelPair::new("a", "=x\n".into()),
            krabka_metrics::MetricLabelPair::new("b", "last".into()),
        ],
        payload: SamplePayload::Float {
            timestamp_ms: 100,
            value: 1.0,
            start_timestamp_ms: Some(50),
        },
        exemplars: Vec::new(),
    };
    let labels = lbls(&[
        crate::test_support::TestLabelPair::new("a", "=x\n"),
        crate::test_support::TestLabelPair::new("b", "last"),
    ]);
    let other = lbls(&[
        crate::test_support::TestLabelPair::new("é", "🦀\0"),
        crate::test_support::TestLabelPair::new("", ""),
        crate::test_support::TestLabelPair::new("x", "y=z\n"),
    ]);
    let record = |tenant: &str, labels: &Labels, payload, exemplars| WalRecord {
        tenant: tenant.into(),
        labels: labels
            .iter()
            .map(|pair| krabka_metrics::MetricLabelPair::new(pair.name.clone(), pair.value.clone()))
            .collect(),
        payload,
        exemplars,
    };
    let records = [
        PositionedWalRecord {
            record: first.clone(),
            partition: PartitionIndex(0),
            offset: Offset(7),
        },
        PositionedWalRecord {
            record: record(
                "t",
                &labels,
                SamplePayload::Hist {
                    timestamp_ms: 200,
                    hist: count_two_sum_three_histogram(),
                },
                Vec::new(),
            ),
            partition: PartitionIndex(0),
            offset: Offset(8),
        },
        PositionedWalRecord {
            record: record(
                "other",
                &other,
                SamplePayload::Float {
                    timestamp_ms: 150,
                    value: 3.0,
                    start_timestamp_ms: None,
                },
                Vec::new(),
            ),
            partition: PartitionIndex(1),
            offset: Offset(20),
        },
        PositionedWalRecord {
            record: record(
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
            partition: PartitionIndex(1),
            offset: Offset(21),
        },
        PositionedWalRecord {
            record: record(
                "t",
                &labels,
                SamplePayload::Exemplars,
                vec![WalExemplar {
                    labels: vec![krabka_metrics::MetricLabelPair::new(
                        "trace_id",
                        "abc".to_string(),
                    )],
                    timestamp_ms: 300,
                    value: 4.0,
                }],
            ),
            partition: PartitionIndex(0),
            offset: Offset(9),
        },
    ];
    head.apply_wal_records_at(
        records
            .iter()
            .map(|entry| WalRecordAt::new(&entry.record, entry.partition, entry.offset)),
    );
    let after = head.snapshot();
    let mut floats = after
        .floats
        .iter()
        .flat_map(|entry| {
            entry.1.iter().map(move |row| FloatRowSnapshot {
                tenant: entry.0.as_str(),
                fingerprint: row.fp,
                labels: row.labels.as_ref().clone(),
                timestamp_ms: row.ts_ms,
                value_bits: row.value.to_bits(),
                start_timestamp_ms: row.start_timestamp_ms,
            })
        })
        .collect::<Vec<_>>();
    floats.sort_by(|left, right| left.tenant.cmp(right.tenant));
    assert2::assert!(
        floats
            == [
                FloatRowSnapshot {
                    tenant: "other",
                    fingerprint: 0x56a3_88ef_237b_8646,
                    labels: other,
                    timestamp_ms: 150,
                    value_bits: 3.0_f64.to_bits(),
                    start_timestamp_ms: None,
                },
                FloatRowSnapshot {
                    tenant: "t",
                    fingerprint: 0xd1e5_33a9_4f60_f896,
                    labels: labels.clone(),
                    timestamp_ms: 100,
                    value_bits: 1.0_f64.to_bits(),
                    start_timestamp_ms: Some(50),
                },
            ]
    );
    let first = after.floats["t"].iter().next().unwrap();
    assert2::assert!(after.hists.len() == 1 && after.hists["t"].len() == 1);
    let hist = after.hists["t"].iter().next().unwrap();
    assert2::assert!(hist.fp == 0xd1e5_33a9_4f60_f896);
    assert2::assert!(hist.labels.as_ref() == &labels);
    assert2::assert!(hist.ts_ms == 200);
    assert2::assert!(hist.hist.as_ref() == &count_two_sum_three_histogram());
    assert2::assert!(Arc::ptr_eq(&first.labels, &hist.labels));
    assert2::assert!(after.exemplars.len() == 1 && after.exemplars["t"].len() == 1);
    let exemplar = after.exemplars["t"].iter().next().unwrap();
    assert2::assert!(exemplar.series_labels.as_ref() == &labels);
    assert2::assert!(
        exemplar.labels.as_ref()
            == &lbls(&[crate::test_support::TestLabelPair::new("trace_id", "abc")])
    );
    assert2::assert!(exemplar.ts_ms == 300);
    assert2::assert!(exemplar.value == 4.0);
    assert2::assert!(Arc::ptr_eq(&first.labels, &exemplar.series_labels));
    assert2::assert!(after.metadata.len() == 1 && after.metadata["t"].len() == 1);
    let m = after.metadata["t"].iter().next().unwrap();
    assert2::assert!(&*m.metric_family_name == "m");
    assert2::assert!(&*m.metric_type == "gauge");
    assert2::assert!(&*m.help == "help");
    assert2::assert!(&*m.unit == "seconds");
    assert2::assert!(
        before.floats.is_empty()
            && before.hists.is_empty()
            && before.metadata.is_empty()
            && before.exemplars.is_empty()
    );
    let partition_zero = after.watermarks.get(&PartitionIndex(0)).unwrap();
    assert2::assert!(partition_zero.low_water_offset == Offset(7));
    assert2::assert!(partition_zero.high_water_offset == Offset(9));
    let partition_one = after.watermarks.get(&PartitionIndex(1)).unwrap();
    assert2::assert!(partition_one.low_water_offset == Offset(20));
    assert2::assert!(partition_one.high_water_offset == Offset(21));
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
    let labels = lbls(&[crate::test_support::TestLabelPair::new("app", "api")]);
    let first = float_record("t", &labels, 100, 1.0);
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        head.apply_wal_records_at(
            std::iter::once(WalRecordAt::new(&first, PartitionIndex(0), Offset(7)))
                .chain(std::iter::from_fn(|| panic!("failed after first record"))),
        );
    }));
    assert2::assert!(panicked.is_err());
    assert2::assert!(head.snapshot().floats.is_empty());
    assert2::assert!(head.high_water_offset(PartitionIndex(0)).is_none());
    let next = float_record("t", &labels, 200, 2.0);
    head.apply_wal_records_at([WalRecordAt::new(&next, PartitionIndex(0), Offset(8))]);
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
    let labels = lbls(&[crate::test_support::TestLabelPair::new("app", "api")]);
    let retired = lbls(&[crate::test_support::TestLabelPair::new("app", "retired")]);
    let record = |labels: &Labels, stamp, value, start| WalRecord {
        tenant: "t".into(),
        labels: labels
            .iter()
            .map(|pair| krabka_metrics::MetricLabelPair::new(pair.name.clone(), pair.value.clone()))
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
    let mut positioned = Vec::with_capacity(records.len());
    for record_index in 0..records.len() {
        positioned.push(WalRecordAt::new(
            &records[record_index],
            PartitionIndex(0),
            Offset([7, 8, 9, 10, 11][record_index]),
        ));
    }
    head.apply_wal_records_at(positioned);
    let batch = head.snapshot();
    let live_labels = Arc::downgrade(&batch.floats["t"].iter().next().unwrap().labels);
    let retired_labels = Arc::downgrade(&batch.floats["t"].iter().last().unwrap().labels);
    let ledger = |snapshot: &InMemoryMetricStore| {
        super::interned_series_labels::summary_ledger(snapshot, "t", &[], 0, 0, 3_000)
    };
    let mut retained = BTreeMap::new();
    retained.insert(
        labels.fingerprint(),
        SummaryLedgerEntry::new(Arc::new(labels.clone()), 1_100, 7.0_f64.to_bits(), Some(0)),
    );
    retained.insert(
        retired.fingerprint(),
        SummaryLedgerEntry::new(Arc::new(retired), 100, 10.0_f64.to_bits(), None),
    );
    let mut surviving = BTreeMap::new();
    surviving.insert(
        labels.fingerprint(),
        SummaryLedgerEntry::new(Arc::new(labels.clone()), 1_100, 7.0_f64.to_bits(), Some(0)),
    );
    assert!(ledger(&before).is_none() && before.watermarks().is_empty());
    assert!(
        ledger(&batch)
            == Some(SummaryLedger {
                entries: retained.clone(),
                total_rows: 5
            })
    );
    assert!(head.prune(2_000).samples_dropped == 2);
    let pruned = head.snapshot();
    assert!(
        ledger(&pruned)
            == Some(SummaryLedger {
                entries: surviving.clone(),
                total_rows: 3
            })
    );
    assert!(
        ledger(&batch)
            == Some(SummaryLedger {
                entries: retained,
                total_rows: 5
            })
    );
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
            std::iter::once(WalRecordAt::new(
                &provisional,
                PartitionIndex(0),
                Offset(12),
            ))
            .chain(std::iter::from_fn(|| {
                panic!("failed after provisional latest")
            })),
        );
    }));
    assert!(panic.is_err());
    assert!(Arc::ptr_eq(&pruned, &head.snapshot()));
    assert!(
        ledger(&head.snapshot())
            == Some(SummaryLedger {
                entries: surviving.clone(),
                total_rows: 3
            })
    );
    assert!(head.high_water_offset(PartitionIndex(0)) == Some(Offset(11)));
    let next = record(&labels, 2_200, 9.0, None);
    head.apply_wal_records_at([WalRecordAt::new(&next, PartitionIndex(0), Offset(13))]);
    let published = head.snapshot();
    let mut published_entries = BTreeMap::new();
    published_entries.insert(
        labels.fingerprint(),
        SummaryLedgerEntry::new(Arc::new(labels), 2_200, 9.0_f64.to_bits(), None),
    );
    assert!(
        ledger(&published)
            == Some(SummaryLedger {
                entries: published_entries,
                total_rows: 4
            })
    );
    assert!(
        ledger(&pruned)
            == Some(SummaryLedger {
                entries: surviving,
                total_rows: 3
            })
    );
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
