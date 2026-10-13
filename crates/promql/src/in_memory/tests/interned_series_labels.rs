use std::{collections::BTreeMap, sync::Arc};

use assert2::assert;
use krabka_blockstore::BlockStore;
use object_store::memory::InMemory;

use super::*;
use crate::{MergedMetricStore, MetricBlockStore};

struct FloatObservationCase<'a> {
    fingerprint: SeriesFingerprint,
    labels: &'a Arc<Labels>,
    timestamp_ms: i64,
    value: f64,
}

struct HistogramObservationCase<'a> {
    fingerprint: SeriesFingerprint,
    labels: &'a Arc<Labels>,
}

struct SummaryWindowCase {
    name: &'static str,
    start: i64,
    end: i64,
    eligible: bool,
    count: usize,
}

struct OwnedFloatObservationCase {
    labels: Arc<Labels>,
    timestamp_ms: i64,
    value: f64,
}

struct StartTimestampCase {
    timestamp_ms: i64,
    value: f64,
    start_timestamp_ms: Option<i64>,
}

struct LabeledStartTimestampCase {
    labels: Labels,
    value: f64,
    start_timestamp_ms: Option<i64>,
}

struct SummaryLedgerEntryCase {
    fingerprint: u64,
    entry: SummaryLedgerEntry,
}

fn summary_entries(
    cases: impl IntoIterator<Item = SummaryLedgerEntryCase>,
) -> BTreeMap<u64, SummaryLedgerEntry> {
    let mut entries = BTreeMap::new();
    for case in cases {
        entries.insert(case.fingerprint, case.entry);
    }
    entries
}

fn labels_by_fingerprint(labels: impl IntoIterator<Item = Labels>) -> BTreeMap<u64, Arc<Labels>> {
    let mut by_fingerprint = BTreeMap::new();
    for labels in labels {
        by_fingerprint.insert(labels.fingerprint(), Arc::new(labels));
    }
    by_fingerprint
}

/// Merges `head` over an empty cold block store.
fn merged_with_empty_cold(head: WalHead) -> MergedMetricStore<MetricBlockStore, WalHead> {
    let blocks = BlockStore::new(
        Arc::new(InMemory::new()),
        url::Url::parse("memory:///").unwrap(),
    );
    MergedMetricStore::new(MetricBlockStore::new(blocks), head)
}

/// Adds to the `t` label cache, under `fp`, an entry whose labels are
/// already dropped.
fn cache_dead_label_entry(store: &mut InMemoryMetricStore, fp: SeriesFingerprint, labels: &Labels) {
    let dead = Arc::new(labels.clone());
    Arc::make_mut(store.series_labels.get_mut("t").unwrap())
        .get_mut(&fp)
        .unwrap()
        .push(Arc::downgrade(&dead));
}

#[test]
fn shared_series_summary_keeps_row_keys_and_histogram_owners() {
    let first = Arc::new(lbls(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "first"),
    ]));
    let second = Arc::new(lbls(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "second"),
    ]));
    let histogram = Arc::new(lbls(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "histogram"),
    ]));
    let mut raw = Vec::new();
    for bytes in [vec![0xff], vec![0xfe], "\u{fffd}".as_bytes().to_vec()] {
        let mut labels = lbls(&[crate::test_support::TestLabelPair::new("__name__", "up")]);
        labels.insert("raw", bytes);
        raw.push(Arc::new(labels));
    }
    let mut store = InMemoryMetricStore::new();
    let mut summary = FloatHeadSummary::default();
    for sample in [
        FloatObservationCase {
            fingerprint: 3,
            labels: &first,
            timestamp_ms: 10,
            value: f64::from_bits(0x7ff0_0000_0000_0002),
        },
        FloatObservationCase {
            fingerprint: 1,
            labels: &second,
            timestamp_ms: 10,
            value: 1.0,
        },
        FloatObservationCase {
            fingerprint: 1,
            labels: &second,
            timestamp_ms: 20,
            value: 2.0,
        },
        FloatObservationCase {
            fingerprint: 2,
            labels: &raw[0],
            timestamp_ms: 20,
            value: f64::NAN,
        },
        FloatObservationCase {
            fingerprint: 6,
            labels: &raw[1],
            timestamp_ms: 20,
            value: f64::INFINITY,
        },
        FloatObservationCase {
            fingerprint: 7,
            labels: &raw[2],
            timestamp_ms: 20,
            value: -0.0,
        },
        FloatObservationCase {
            fingerprint: 5,
            labels: &first,
            timestamp_ms: 0,
            value: 3.0,
        },
    ] {
        assert!(sample.fingerprint != sample.labels.fingerprint());
        let row = FloatRow {
            fp: sample.fingerprint,
            labels: Arc::clone(sample.labels),
            ts_ms: sample.timestamp_ms,
            value: sample.value,
            start_timestamp_ms: None,
        };
        summary.observe(&row);
        store.floats.entry("t".into()).or_default().push(row);
    }
    store
        .float_head_summaries
        .insert("t".into(), Arc::new(summary));
    for sample in [
        HistogramObservationCase {
            fingerprint: 1,
            labels: &histogram,
        },
        HistogramObservationCase {
            fingerprint: 4,
            labels: &histogram,
        },
        HistogramObservationCase {
            fingerprint: 5,
            labels: &histogram,
        },
    ] {
        store.hists.entry("t".into()).or_default().push(HistRow {
            fp: sample.fingerprint,
            labels: Arc::clone(sample.labels),
            ts_ms: 15,
            hist: Arc::new(count_two_sum_three_histogram()),
        });
    }
    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    assert!(summary_ledger(&store, "t", &matchers, 10, 10, 20).is_some());
    let mut expected = vec![
        Arc::clone(&second),
        Arc::clone(&raw[0]),
        Arc::clone(&first),
        Arc::clone(&histogram),
        Arc::clone(&histogram),
        Arc::clone(&raw[1]),
        Arc::clone(&raw[2]),
    ];
    expected.sort_by_key(|labels| labels.fingerprint());
    let actual = store.matched_series_shared("t", &matchers, 10, 20).unwrap();
    assert!(actual == expected);
    for index in 0..actual.len() {
        assert!(Arc::ptr_eq(&actual[index], &expected[index]));
    }
    let shard = [LabelMatcher::new("__query_shard__", MatchOp::Eq, "1_of_2")];
    assert!(summary_ledger(&store, "t", &shard, 10, 10, 20).is_some());
    assert!(
        store.matched_series_shared("t", &shard, 10, 20).unwrap()
            == vec![
                Arc::clone(&raw[0]),
                Arc::clone(&histogram),
                Arc::clone(&raw[1])
            ]
    );
    let selected = [LabelMatcher::new("raw", MatchOp::Eq, vec![0xff])];
    assert!(
        store.matched_series_shared("t", &selected, 10, 20).unwrap() == vec![Arc::clone(&raw[0])]
    );
}

#[test]
fn shared_series_summary_preserves_window_fallback_and_snapshot_owners() {
    for case in [
        SummaryWindowCase {
            name: "shared",
            start: 10,
            end: 20,
            eligible: true,
            count: 1,
        },
        SummaryWindowCase {
            name: "boundary",
            start: 20,
            end: 20,
            eligible: true,
            count: 1,
        },
        SummaryWindowCase {
            name: "empty",
            start: 21,
            end: 30,
            eligible: true,
            count: 0,
        },
        SummaryWindowCase {
            name: "past",
            start: 10,
            end: 10,
            eligible: false,
            count: 1,
        },
        SummaryWindowCase {
            name: "future",
            start: 10,
            end: 20,
            eligible: false,
            count: 1,
        },
        SummaryWindowCase {
            name: "ambiguous",
            start: 20,
            end: 20,
            eligible: false,
            count: 1,
        },
        SummaryWindowCase {
            name: "missing",
            start: 10,
            end: 20,
            eligible: false,
            count: 1,
        },
        SummaryWindowCase {
            name: "count",
            start: 10,
            end: 20,
            eligible: false,
            count: 1,
        },
        SummaryWindowCase {
            name: "pruned",
            start: 20,
            end: 20,
            eligible: true,
            count: 1,
        },
    ] {
        let first = Arc::new(lbls(&[crate::test_support::TestLabelPair::new(
            "__name__", "up",
        )]));
        let mut expected = Arc::clone(&first);
        let mut store = InMemoryMetricStore::with_retention(millis(5));
        store.push_float("t", Arc::clone(&first), 10, 1.0);
        if case.name == "ambiguous" {
            expected = Arc::new(first.as_ref().clone());
            let row = FloatRow {
                fp: first.fingerprint(),
                labels: Arc::clone(&expected),
                ts_ms: 20,
                value: 2.0,
                start_timestamp_ms: None,
            };
            Arc::make_mut(store.float_head_summaries.get_mut("t").unwrap()).observe(&row);
            store.floats.get_mut("t").unwrap().push(row);
        } else {
            store.push_float("t", Arc::clone(&first), 20, 2.0);
        }
        match case.name {
            "future" => store.push_float("t", Arc::clone(&first), 30, 3.0),
            "missing" => {
                store.float_head_summaries.remove("t");
            }
            "count" => store.floats.get_mut("t").unwrap().push(FloatRow {
                fp: first.fingerprint(),
                labels: Arc::clone(&first),
                ts_ms: 25,
                value: 3.0,
                start_timestamp_ms: None,
            }),
            "pruned" => {
                assert!(store.prune(25).samples_dropped == 1);
            }
            _ => {}
        }
        assert!(
            summary_ledger(&store, "t", &[], case.start, case.start, case.end).is_some()
                == case.eligible,
            "{}",
            case.name
        );
        let snapshot = store.clone();
        let actual = snapshot
            .matched_series_shared("t", &[], case.start, case.end)
            .unwrap();
        let expected = if case.count == 0 {
            Vec::new()
        } else {
            vec![expected]
        };
        assert!(actual == expected, "{}", case.name);
        for index in 0..actual.len() {
            assert!(
                Arc::ptr_eq(&actual[index], &expected[index]),
                "{}",
                case.name
            );
        }
        store.delete_tenant("t");
        assert!(
            store
                .matched_series_shared("t", &[], case.start, case.end)
                .unwrap()
                .is_empty()
        );
        assert!(
            snapshot
                .matched_series_shared("t", &[], case.start, case.end)
                .unwrap()
                == expected
        );
    }
    let empty = InMemoryMetricStore::new();
    let invalid = [LabelMatcher::new("__name__", MatchOp::Re, "[")];
    assert!(matches!(
        empty.matched_series_shared("missing", &invalid, 0, 20),
        Err(PromqlError::Plan(_))
    ));
}

#[test]
fn labels_are_shared_across_records_and_sample_types_within_one_tenant() {
    let mut store = InMemoryMetricStore::new();
    let labels = lbls(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    store.push_float("t", labels.clone(), 100, 1.0);
    store.push_float("t", labels.clone(), 200, 2.0);
    store.push_histogram("t", labels.clone(), 300, count_two_sum_three_histogram());
    store.push_exemplar("t", labels.clone(), Labels::new(), 400, 3.0);
    store.push_float("other", labels, 500, 4.0);

    let floats = store.floats["t"].iter().collect::<Vec<_>>();
    let hist = store.hists["t"].iter().next().unwrap();
    let exemplar = store.exemplars["t"].iter().next().unwrap();
    let other = store.floats["other"].iter().next().unwrap();
    check!(Arc::ptr_eq(&floats[0].labels, &floats[1].labels));
    check!(Arc::ptr_eq(&floats[0].labels, &hist.labels));
    check!(Arc::ptr_eq(&floats[0].labels, &exemplar.series_labels));
    check!(!Arc::ptr_eq(&floats[0].labels, &other.labels));
    check!(
        floats
            .iter()
            .map(|row| (row.ts_ms, row.value))
            .collect::<Vec<_>>()
            == vec![(100, 1.0), (200, 2.0)]
    );
}

#[test]
fn pruning_frees_labels_after_the_last_snapshot_releases_them() {
    let mut store = InMemoryMetricStore::with_retention(secs(1));
    store.push_float(
        "t",
        lbls(&[crate::test_support::TestLabelPair::new("__name__", "up")]),
        100,
        1.0,
    );
    let labels = Arc::downgrade(&store.floats["t"].iter().next().unwrap().labels);
    let snapshot = store.clone();
    check!(store.prune(2_000).samples_dropped == 1);
    check!(store.series_labels.is_empty());
    check!(labels.upgrade().is_some());
    drop(snapshot);
    check!(labels.upgrade().is_none());
}

#[test]
fn live_label_hits_share_the_cache_with_snapshots_but_dead_entries_are_cleaned() {
    let labels = lbls(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    let fp = labels.fingerprint();
    let mut store = InMemoryMetricStore::new();
    store.push_float("t", labels.clone(), 100, 1.0);
    let snapshot = store.clone();
    store.push_float("t", labels.clone(), 200, 2.0);
    store.push_histogram("t", labels.clone(), 300, count_two_sum_three_histogram());
    store.push_exemplar("t", labels.clone(), Labels::new(), 400, 3.0);
    assert!(Arc::ptr_eq(
        &store.series_labels["t"],
        &snapshot.series_labels["t"]
    ));
    assert!(store.series_labels["t"][&fp].len() == 1);
    assert!(
        store.floats["t"]
            .iter()
            .map(|row| (row.ts_ms, row.value))
            .collect::<Vec<_>>()
            == vec![(100, 1.0), (200, 2.0)]
    );
    assert!(
        snapshot.floats["t"]
            .iter()
            .map(|row| (row.ts_ms, row.value))
            .collect::<Vec<_>>()
            == vec![(100, 1.0)]
    );
    assert!(!snapshot.hists.contains_key("t"));
    assert!(!snapshot.exemplars.contains_key("t"));

    cache_dead_label_entry(&mut store, fp, &labels);
    let dirty_snapshot = store.clone();
    store.push_float("t", labels.clone(), 500, 4.0);
    assert!(!Arc::ptr_eq(
        &store.series_labels["t"],
        &dirty_snapshot.series_labels["t"]
    ));
    assert!(store.series_labels["t"][&fp].len() == 1);
    assert!(dirty_snapshot.series_labels["t"][&fp].len() == 2);
    assert!(store.floats["t"].iter().last().unwrap().labels.as_ref() == &labels);
    assert!(Arc::ptr_eq(
        &store.floats["t"].iter().next().unwrap().labels,
        &store.floats["t"].iter().last().unwrap().labels
    ));
}

#[test]
fn a_fingerprint_collision_does_not_intern_different_labels_together() {
    let wanted = lbls(&[crate::test_support::TestLabelPair::new("__name__", "up")]);
    let other = Arc::new(lbls(&[crate::test_support::TestLabelPair::new(
        "__name__", "down",
    )]));
    let mut store = InMemoryMetricStore::new();
    // Force a hash collision at the cache seam; the equality check must still
    // distinguish the label sets without relying on finding a real collision.
    let mut label_cache = HashMap::new();
    label_cache.insert(wanted.fingerprint(), vec![Arc::downgrade(&other)]);
    store
        .series_labels
        .insert("t".into(), Arc::new(label_cache));
    store.push_float("t", wanted.clone(), 100, 1.0);
    let row = store.floats["t"].iter().next().unwrap();
    check!(row.labels.as_ref() == &wanted);
    check!(!Arc::ptr_eq(&row.labels, &other));
}

#[test]
fn wal_replay_keeps_byte_labels_tenants_and_snapshots_with_shared_series() {
    for bytes in [
        b"api".to_vec(),
        vec![0xff],
        vec![0xfe],
        "\u{fffd}".as_bytes().to_vec(),
        vec![0, 0xff, 0],
    ] {
        let labels = Labels::from_pairs([
            krabka_metrics::MetricLabelPair::new("__name__", b"up".to_vec()),
            krabka_metrics::MetricLabelPair::new("job", bytes.clone()),
        ]);
        let fp = labels.fingerprint();
        let sorted = vec![
            krabka_metrics::MetricLabelPair::new("__name__", b"up".to_vec().into()),
            krabka_metrics::MetricLabelPair::new("job", bytes.clone().into()),
        ];
        let mut reversed = sorted.clone();
        reversed.reverse();
        let duplicate = vec![
            krabka_metrics::MetricLabelPair::new("job", b"old".to_vec().into()),
            krabka_metrics::MetricLabelPair::new("__name__", b"up".to_vec().into()),
            krabka_metrics::MetricLabelPair::new("job", bytes.into()),
        ];
        for input in [sorted, reversed, duplicate] {
            let mut store = InMemoryMetricStore::new();
            store.push_float_with_start_timestamp("t", labels.clone(), 10, 1.0, Some(5));
            let snapshot = store.clone();
            let mut record = WalRecord {
                tenant: "t".to_owned(),
                labels: input,
                payload: SamplePayload::Float {
                    timestamp_ms: 20,
                    value: 3.0,
                    start_timestamp_ms: Some(7),
                },
                exemplars: vec![WalExemplar {
                    labels: vec![krabka_metrics::MetricLabelPair::new(
                        "trace_id",
                        "abc".to_owned(),
                    )],
                    timestamp_ms: 21,
                    value: 4.0,
                }],
            };
            store.apply_wal_record(&record);
            record.payload = SamplePayload::Hist {
                timestamp_ms: 30,
                hist: count_two_sum_three_histogram(),
            };
            record.exemplars.clear();
            store.apply_wal_record(&record);
            record.tenant = "other".to_owned();
            store.apply_wal_record(&record);

            let floats = store.floats["t"].iter().collect::<Vec<_>>();
            let histogram = store.hists["t"].iter().next().unwrap();
            let exemplar = store.exemplars["t"].iter().next().unwrap();
            let other = store.hists["other"].iter().next().unwrap();
            assert!(
                floats
                    .iter()
                    .map(|row| (
                        row.fp,
                        row.labels.as_ref().clone(),
                        row.ts_ms,
                        row.value.to_bits(),
                        row.start_timestamp_ms,
                    ))
                    .collect::<Vec<_>>()
                    == vec![
                        (fp, labels.clone(), 10, 1.0_f64.to_bits(), Some(5)),
                        (fp, labels.clone(), 20, 3.0_f64.to_bits(), Some(7)),
                    ]
            );
            assert!(
                (
                    histogram.fp,
                    histogram.labels.as_ref(),
                    histogram.ts_ms,
                    histogram.hist.as_ref()
                ) == (fp, &labels, 30, &count_two_sum_three_histogram())
            );
            assert!(
                (
                    exemplar.series_labels.as_ref(),
                    exemplar.labels.as_ref(),
                    exemplar.ts_ms,
                    exemplar.value.to_bits(),
                ) == (
                    &labels,
                    &lbls(&[crate::test_support::TestLabelPair::new("trace_id", "abc")]),
                    21,
                    4.0_f64.to_bits()
                )
            );
            assert!(Arc::ptr_eq(&floats[0].labels, &floats[1].labels));
            assert!(Arc::ptr_eq(&floats[0].labels, &histogram.labels));
            assert!(Arc::ptr_eq(&floats[0].labels, &exemplar.series_labels));
            assert!(!Arc::ptr_eq(&floats[0].labels, &other.labels));
            assert!(Arc::ptr_eq(
                &store.series_labels["t"],
                &snapshot.series_labels["t"]
            ));
            assert!(snapshot.floats["t"].len() == 1);
            assert!(snapshot.hists.is_empty() && snapshot.exemplars.is_empty());
        }
    }
}

#[test]
fn wal_label_hits_keep_collisions_and_clean_dead_entries_without_changing_snapshots() {
    let labels = Labels::from_pairs([
        krabka_metrics::MetricLabelPair::new("__name__", b"up".to_vec()),
        krabka_metrics::MetricLabelPair::new("job", vec![0xff]),
    ]);
    let fp = labels.fingerprint();
    let wrong = Arc::new(Labels::from_pairs([
        krabka_metrics::MetricLabelPair::new("__name__", b"up".to_vec()),
        krabka_metrics::MetricLabelPair::new("job", vec![0xfe]),
    ]));
    let mut store = InMemoryMetricStore::new();
    // These byte values have the same JSON view. Full byte equality still
    // distinguishes them when their cache keys collide.
    let mut label_cache = HashMap::new();
    label_cache.insert(fp, vec![Arc::downgrade(&wrong)]);
    store
        .series_labels
        .insert("t".to_owned(), Arc::new(label_cache));
    let mut record = WalRecord {
        tenant: "t".to_owned(),
        labels: vec![
            krabka_metrics::MetricLabelPair::new("__name__", b"up".to_vec().into()),
            krabka_metrics::MetricLabelPair::new("job", vec![0xff].into()),
        ],
        payload: SamplePayload::Float {
            timestamp_ms: 10,
            value: 1.0,
            start_timestamp_ms: Some(5),
        },
        exemplars: Vec::new(),
    };
    store.apply_wal_record(&record);
    let snapshot = store.clone();
    cache_dead_label_entry(&mut store, fp, &labels);
    let dirty_snapshot = store.clone();
    record.payload = SamplePayload::Float {
        timestamp_ms: 20,
        value: 3.0,
        start_timestamp_ms: Some(7),
    };
    store.apply_wal_record(&record);
    let rows = store.floats["t"].iter().collect::<Vec<_>>();
    assert!(
        rows.iter()
            .all(|row| row.fp == fp && row.labels.as_ref() == &labels)
    );
    assert!(Arc::ptr_eq(&rows[0].labels, &rows[1].labels));
    assert!(!Arc::ptr_eq(&rows[1].labels, &wrong));
    assert!(store.series_labels["t"][&fp].len() == 2);
    assert!(dirty_snapshot.series_labels["t"][&fp].len() == 3);
    assert!(!Arc::ptr_eq(
        &store.series_labels["t"],
        &dirty_snapshot.series_labels["t"]
    ));
    assert!(snapshot.floats["t"].len() == 1 && dirty_snapshot.floats["t"].len() == 1);
    let held = Arc::downgrade(&rows[0].labels);
    store.delete_tenant("t");
    assert!(held.upgrade().is_some());
    drop(snapshot);
    drop(dirty_snapshot);
    assert!(held.upgrade().is_none());
}

#[tokio::test]
async fn equal_fingerprints_do_not_reuse_matchers_for_different_labels() {
    let wanted = Arc::new(Labels::from_pairs([
        krabka_metrics::MetricLabelPair::new("__name__", "up"),
        krabka_metrics::MetricLabelPair::new("job", "api"),
    ]));
    let other = Arc::new(Labels::from_pairs([
        krabka_metrics::MetricLabelPair::new("__name__", "down"),
        krabka_metrics::MetricLabelPair::new("job", "api"),
    ]));
    let fp = wanted.fingerprint();
    let mut hot = InMemoryMetricStore::new();
    // Force collisions at the row seam. Reusing the last successful matcher
    // by fingerprint alone would incorrectly select the newer down sample.
    for sample in [
        OwnedFloatObservationCase {
            labels: Arc::clone(&wanted),
            timestamp_ms: 10_000,
            value: 3.0,
        },
        OwnedFloatObservationCase {
            labels: Arc::new(wanted.as_ref().clone()),
            timestamp_ms: 11_000,
            value: 7.0,
        },
        OwnedFloatObservationCase {
            labels: other,
            timestamp_ms: 12_000,
            value: 99.0,
        },
    ] {
        hot.floats
            .entry("tenant-a".into())
            .or_default()
            .push(FloatRow {
                fp,
                labels: sample.labels,
                ts_ms: sample.timestamp_ms,
                value: sample.value,
                start_timestamp_ms: None,
            });
    }
    let store = merged_with_empty_cold(WalHead::from_store(hot));
    let scan = store
        .try_latest_float_scan(
            "tenant-a",
            &[LabelMatcher::new("__name__", MatchOp::Eq, "up")],
            9_000,
            9_001,
            12_000,
            2,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(scan.samples == vec![float_sample_row(fp, 11_000, 7.0, None)]);
    assert!(scan.labels.len() == 1);
    assert!(scan.labels[&fp].as_ref() == wanted.as_ref());
    assert!(Arc::ptr_eq(&scan.labels[&fp], &wanted));
}

#[tokio::test]
async fn cold_labels_do_not_hide_different_hot_labels_with_the_same_row_id() {
    let cold_labels = Arc::new(Labels::from_pairs([
        krabka_metrics::MetricLabelPair::new("__name__", "up"),
        krabka_metrics::MetricLabelPair::new("job", "api"),
    ]));
    let hot_labels = Arc::new(Labels::from_pairs([
        krabka_metrics::MetricLabelPair::new("__name__", "up"),
        krabka_metrics::MetricLabelPair::new("job", "worker"),
    ]));
    let cold_fp = cold_labels.fingerprint();
    let hot_fp = hot_labels.fingerprint();
    let mut blocks = BlockStore::new(
        Arc::new(InMemory::new()),
        url::Url::parse("memory:///").unwrap(),
    );
    blocks
        .index_mut()
        .add_series("tenant-a", cold_fp ^ 1, &cold_labels.utf8_projection());
    let mut hot = InMemoryMetricStore::new();
    // The row ID shares the cold label key. The hot labels still need their
    // own canonical key in the returned label map.
    hot.floats
        .entry("tenant-a".into())
        .or_default()
        .push(FloatRow {
            fp: cold_fp,
            labels: Arc::clone(&hot_labels),
            ts_ms: 10_000,
            value: 7.0,
            start_timestamp_ms: None,
        });
    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let generic = hot
        .series_shared_by_fingerprint("tenant-a", &matchers, 9_000, 11_000)
        .await
        .unwrap();
    assert!(generic == BTreeMap::from([(hot_fp, Arc::clone(&hot_labels))]));
    assert!(Arc::ptr_eq(&generic[&hot_fp], &hot_labels));
    let store = MergedMetricStore::new(MetricBlockStore::new(blocks), WalHead::from_store(hot));
    let mapped = store
        .series_shared_by_fingerprint("tenant-a", &matchers, 9_000, 11_000)
        .await
        .unwrap();
    assert!(
        mapped
            == BTreeMap::from([
                (cold_fp, Arc::clone(&cold_labels)),
                (hot_fp, Arc::clone(&hot_labels)),
            ])
    );
    assert!(Arc::ptr_eq(&mapped[&hot_fp], &hot_labels));
    let scan = store
        .try_latest_float_scan(
            "tenant-a",
            &[LabelMatcher::new("__name__", MatchOp::Eq, "up")],
            9_000,
            9_001,
            11_000,
            1,
        )
        .await
        .unwrap()
        .unwrap();
    assert!(scan.samples == vec![float_sample_row(cold_fp, 10_000, 7.0, None)]);
    assert!(
        scan.labels == BTreeMap::from([(cold_fp, Arc::clone(&cold_labels)), (hot_fp, hot_labels),])
    );
    assert!(!Arc::ptr_eq(&scan.labels[&cold_fp], &cold_labels));
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(super) struct SummaryLedgerEntry {
    pub(super) labels: Arc<Labels>,
    pub(super) timestamp_ms: i64,
    pub(super) value_bits: u64,
    pub(super) start_timestamp_ms: Option<i64>,
}

impl SummaryLedgerEntry {
    pub(super) fn new(
        labels: Arc<Labels>,
        timestamp_ms: i64,
        value_bits: u64,
        start_timestamp_ms: Option<i64>,
    ) -> Self {
        Self {
            labels,
            timestamp_ms,
            value_bits,
            start_timestamp_ms,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct SummaryLedger {
    pub(super) entries: BTreeMap<u64, SummaryLedgerEntry>,
    pub(super) total_rows: usize,
}

pub(super) fn summary_ledger(
    store: &InMemoryMetricStore,
    tenant: &str,
    matchers: &[LabelMatcher],
    label_start_ms: i64,
    sample_start_ms: i64,
    end_ms: i64,
) -> Option<SummaryLedger> {
    let prepared = prepare_matchers(matchers).unwrap();
    let matching = store.float_head_summaries.get(tenant)?.matching_series(
        store.floats.get(tenant)?.len(),
        &prepared,
        label_start_ms,
        sample_start_ms,
        end_ms,
    )?;
    let mut entries = BTreeMap::new();
    for series in matching.series {
        entries.insert(
            series.latest.fingerprint,
            SummaryLedgerEntry::new(
                Arc::clone(&series.labels),
                series.latest.timestamp_ms,
                series.latest.value.to_bits(),
                series.latest.start_timestamp_ms,
            ),
        );
    }
    Some(SummaryLedger {
        entries,
        total_rows: matching.total_rows,
    })
}

#[tokio::test]
async fn eligible_summary_keeps_old_rows_boundaries_raw_counts_and_future_fallback() {
    let labels = lbls(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    let boundary = lbls(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "boundary"),
    ]);
    let histogram = lbls(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "histogram"),
    ]);
    let mut hot = InMemoryMetricStore::new();
    for sample in [
        StartTimestampCase {
            timestamp_ms: 1_000,
            value: 1.0,
            start_timestamp_ms: None,
        },
        StartTimestampCase {
            timestamp_ms: 9_000,
            value: 3.0,
            start_timestamp_ms: None,
        },
        StartTimestampCase {
            timestamp_ms: 11_000,
            value: 7.0,
            start_timestamp_ms: Some(0),
        },
        StartTimestampCase {
            timestamp_ms: 11_000,
            value: 99.0,
            start_timestamp_ms: None,
        },
        StartTimestampCase {
            timestamp_ms: 10_000,
            value: 4.0,
            start_timestamp_ms: None,
        },
    ] {
        hot.push_float_with_start_timestamp(
            "t",
            labels.clone(),
            sample.timestamp_ms,
            sample.value,
            sample.start_timestamp_ms,
        );
    }
    hot.push_float("t", boundary.clone(), 8_000, 40.0);
    hot.push_float("t", boundary.clone(), 9_000, 42.0);
    hot.push_histogram(
        "t",
        histogram.clone(),
        9_000,
        count_two_sum_three_histogram(),
    );
    hot.push_float(
        "t",
        lbls(&[crate::test_support::TestLabelPair::new("__name__", "down")]),
        13_000,
        99.0,
    );
    hot.push_float("other", labels.clone(), 11_000, 99.0);
    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let expected = summary_entries([
        SummaryLedgerEntryCase {
            fingerprint: labels.fingerprint(),
            entry: SummaryLedgerEntry::new(
                Arc::new(labels.clone()),
                11_000,
                7.0_f64.to_bits(),
                Some(0),
            ),
        },
        SummaryLedgerEntryCase {
            fingerprint: boundary.fingerprint(),
            entry: SummaryLedgerEntry::new(
                Arc::new(boundary.clone()),
                9_000,
                42.0_f64.to_bits(),
                None,
            ),
        },
    ]);
    // This positive assertion cannot be satisfied by the ordinary row fallback.
    // Old retained rows count conservatively; labels-only rows contribute zero.
    assert!(
        summary_ledger(&hot, "t", &matchers, 9_000, 9_001, 12_000)
            == Some(SummaryLedger {
                entries: expected,
                total_rows: 5
            })
    );
    let head = WalHead::from_store(hot);
    let store = merged_with_empty_cold(head.clone());
    let scan = store
        .try_latest_float_scan("t", &matchers, 9_000, 9_001, 12_000, 5)
        .await
        .unwrap()
        .unwrap();
    assert!(scan.samples == vec![float_sample_row(labels.fingerprint(), 11_000, 7.0, Some(0))]);
    assert!(scan.labels == labels_by_fingerprint([labels.clone(), boundary, histogram]));
    // A conservative summary cap must retain the original hot-row path,
    // whose exact sample-window count is three, rather than disable it.
    let capped = store
        .try_latest_float_scan("t", &matchers, 9_000, 9_001, 12_000, 4)
        .await
        .unwrap()
        .unwrap();
    assert!(capped.samples == scan.samples && capped.labels == scan.labels);
    assert!(
        store
            .try_latest_float_scan("t", &matchers, 9_000, 9_001, 12_000, 2)
            .await
            .unwrap()
            .is_none()
    );
    head.update(|hot| hot.push_float("t", labels.clone(), 13_000, 99.0));
    assert!(summary_ledger(&head.snapshot(), "t", &matchers, 9_000, 9_001, 12_000).is_none());
    let historical = store
        .try_latest_float_scan("t", &matchers, 9_000, 9_001, 12_000, 4)
        .await
        .unwrap()
        .unwrap();
    assert!(historical.samples == scan.samples && historical.labels == scan.labels);
}

#[tokio::test]
async fn latest_summary_preserves_first_tie_stale_bits_and_optional_zero_start() {
    let finite = lbls(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "finite"),
    ]);
    let stale = lbls(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "stale"),
    ]);
    let zero = lbls(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "zero"),
    ]);
    let mut hot = InMemoryMetricStore::new();
    for sample in [
        LabeledStartTimestampCase {
            labels: finite.clone(),
            value: 7.0,
            start_timestamp_ms: Some(0),
        },
        LabeledStartTimestampCase {
            labels: finite.clone(),
            value: 99.0,
            start_timestamp_ms: None,
        },
        LabeledStartTimestampCase {
            labels: stale.clone(),
            value: f64::from_bits(0x7ff0_0000_0000_0002),
            start_timestamp_ms: Some(0),
        },
        LabeledStartTimestampCase {
            labels: stale.clone(),
            value: 99.0,
            start_timestamp_ms: None,
        },
        LabeledStartTimestampCase {
            labels: zero.clone(),
            value: -0.0,
            start_timestamp_ms: None,
        },
        LabeledStartTimestampCase {
            labels: zero.clone(),
            value: 99.0,
            start_timestamp_ms: Some(0),
        },
    ] {
        hot.push_float_with_start_timestamp(
            "t",
            sample.labels,
            11_000,
            sample.value,
            sample.start_timestamp_ms,
        );
    }
    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let expected = summary_entries([
        SummaryLedgerEntryCase {
            fingerprint: finite.fingerprint(),
            entry: SummaryLedgerEntry::new(
                Arc::new(finite.clone()),
                11_000,
                7.0_f64.to_bits(),
                Some(0),
            ),
        },
        SummaryLedgerEntryCase {
            fingerprint: stale.fingerprint(),
            entry: SummaryLedgerEntry::new(
                Arc::new(stale.clone()),
                11_000,
                0x7ff0_0000_0000_0002,
                Some(0),
            ),
        },
        SummaryLedgerEntryCase {
            fingerprint: zero.fingerprint(),
            entry: SummaryLedgerEntry::new(
                Arc::new(zero.clone()),
                11_000,
                (-0.0_f64).to_bits(),
                None,
            ),
        },
    ]);
    assert!(
        summary_ledger(&hot, "t", &matchers, 9_000, 9_001, 12_000)
            == Some(SummaryLedger {
                entries: expected,
                total_rows: 6
            })
    );
    let store = merged_with_empty_cold(WalHead::from_store(hot));
    let scan = store
        .try_latest_float_scan("t", &matchers, 9_000, 9_001, 12_000, 6)
        .await
        .unwrap()
        .unwrap();
    let mut expected_samples = vec![
        krabka_metrics::FloatSampleRow {
            fingerprint: finite.fingerprint(),
            timestamp_ms: 11_000,
            value: 7.0,
            start_timestamp_ms: Some(0),
        },
        krabka_metrics::FloatSampleRow {
            fingerprint: stale.fingerprint(),
            timestamp_ms: 11_000,
            value: f64::from_bits(0x7ff0_0000_0000_0002),
            start_timestamp_ms: Some(0),
        },
        krabka_metrics::FloatSampleRow {
            fingerprint: zero.fingerprint(),
            timestamp_ms: 11_000,
            value: -0.0,
            start_timestamp_ms: None,
        },
    ];
    expected_samples.sort_by_key(|row| row.fingerprint);
    assert!(scan.samples.len() == expected_samples.len());
    for index in 0..scan.samples.len() {
        let actual = &scan.samples[index];
        let expected = &expected_samples[index];
        assert!(
            actual.fingerprint == expected.fingerprint
                && actual.timestamp_ms == expected.timestamp_ms
                && actual.value.to_bits() == expected.value.to_bits()
                && actual.start_timestamp_ms == expected.start_timestamp_ms
        );
    }
    assert!(scan.labels == labels_by_fingerprint([finite, stale, zero]));
}

/// Prunes `hot` at 10s, which drops one sample, and checks that the latest
/// `up` sample in `(9s, 12s]` then takes the row path and answers the 7 at 11s
/// under the very `owner` label set.
async fn assert_pruned_scan_shares_owner(mut hot: InMemoryMetricStore, owner: &Arc<Labels>) {
    let fp = owner.fingerprint();
    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    assert!(hot.prune(10_000).samples_dropped == 1);
    assert!(summary_ledger(&hot, "t", &matchers, 9_000, 9_001, 12_000).is_none());
    let store = merged_with_empty_cold(WalHead::from_store(hot));
    let scan = store
        .try_latest_float_scan("t", &matchers, 9_000, 9_001, 12_000, 2)
        .await
        .unwrap()
        .unwrap();
    assert!(scan.samples == vec![float_sample_row(fp, 11_000, 7.0, Some(0))]);
    assert!(scan.labels == BTreeMap::from([(fp, Arc::clone(owner))]));
    assert!(Arc::ptr_eq(&scan.labels[&fp], owner));
}

#[tokio::test]
async fn summary_falls_back_after_direct_append_and_rebuilt_label_identity_collision() {
    let wanted = lbls(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    let other = Arc::new(lbls(&[
        crate::test_support::TestLabelPair::new("__name__", "down"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]));
    let fp = wanted.fingerprint();
    let mut hot = InMemoryMetricStore::with_retention(secs(1));
    hot.push_float("t", wanted.clone(), 100, 1.0);
    hot.push_float("t", wanted.clone(), 10_000, 3.0);
    let wanted_arc = Arc::clone(&hot.floats["t"].iter().last().unwrap().labels);
    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    assert!(summary_ledger(&hot, "t", &matchers, 9_000, 9_001, 12_000).is_some());
    hot.floats.get_mut("t").unwrap().push(FloatRow {
        fp,
        labels: Arc::new(wanted.clone()),
        ts_ms: 11_000,
        value: 7.0,
        start_timestamp_ms: Some(0),
    });
    assert!(summary_ledger(&hot, "t", &matchers, 9_000, 9_001, 12_000).is_none());
    hot.floats.get_mut("t").unwrap().push(FloatRow {
        fp,
        labels: other,
        ts_ms: 12_000,
        value: 99.0,
        start_timestamp_ms: None,
    });
    // Pruning rebuilds a complete summary; the two distinct label Arcs still
    // require the row path, including the rejected newer 'down' sample.
    assert!(*wanted_arc == wanted);
    assert_pruned_scan_shares_owner(hot, &wanted_arc).await;

    let wanted = lbls(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    let in_window = Arc::new(wanted.clone());
    let mut hot = InMemoryMetricStore::with_retention(secs(3));
    hot.push_float("t", wanted, 8_000, 1.0);
    hot.push_float(
        "other",
        lbls(&[crate::test_support::TestLabelPair::new(
            "__name__", "expired",
        )]),
        100,
        1.0,
    );
    hot.floats.get_mut("t").unwrap().push(FloatRow {
        fp,
        labels: Arc::clone(&in_window),
        ts_ms: 11_000,
        value: 7.0,
        start_timestamp_ms: Some(0),
    });
    assert_pruned_scan_shares_owner(hot, &in_window).await;
}

#[tokio::test]
async fn byte_labels_remain_distinct_in_interning_and_latest_scan_snapshots() {
    let values = [vec![0xff], vec![0xfe], "\u{fffd}".as_bytes().to_vec()];
    let mut hot = InMemoryMetricStore::new();
    for index in 0..values.len() {
        let bytes = &values[index];
        let mut labels = lbls(&[crate::test_support::TestLabelPair::new("__name__", "up")]);
        labels.insert("raw", bytes.clone());
        hot.push_float(
            "tenant-a",
            labels.clone(),
            10_000,
            f64::from(u32::try_from(index).unwrap()) + 1.0,
        );
        hot.push_float(
            "tenant-a",
            labels,
            11_000,
            f64::from(u32::try_from(index).unwrap()) + 4.0,
        );
    }
    let values_at_latest = [4.0_f64, 5.0, 6.0];
    let mut expected_entries = Vec::new();
    for index in 0..values.len() {
        let bytes = &values[index];
        let value = values_at_latest[index];
        let mut labels = lbls(&[crate::test_support::TestLabelPair::new("__name__", "up")]);
        labels.insert("raw", bytes.clone());
        expected_entries.push(SummaryLedgerEntryCase {
            fingerprint: labels.fingerprint(),
            entry: SummaryLedgerEntry::new(Arc::new(labels), 11_000, value.to_bits(), None),
        });
    }
    let expected = summary_entries(expected_entries);
    assert!(
        summary_ledger(&hot, "tenant-a", &[], 9_000, 9_001, 11_000)
            == Some(SummaryLedger {
                entries: expected,
                total_rows: 6
            })
    );
    let head = WalHead::from_store(hot);
    let snapshot = head.snapshot();
    let rows = snapshot.floats["tenant-a"].iter().collect::<Vec<_>>();
    for pair in rows.as_chunks::<2>().0 {
        assert!(Arc::ptr_eq(&pair[0].labels, &pair[1].labels));
    }
    assert!(snapshot.series_labels["tenant-a"].len() == 3);
    let store = merged_with_empty_cold(head.clone());
    let captured = store
        .try_latest_float_scan("tenant-a", &[], 9_000, 9_001, 11_000, 6)
        .await
        .unwrap()
        .unwrap();
    assert!(captured.samples.len() == 3 && captured.labels.len() == 3);
    let mapped = store
        .series_shared_by_fingerprint("tenant-a", &[], 9_000, 11_000)
        .await
        .unwrap();
    assert!(mapped.len() == 3);
    for row in rows.as_chunks::<2>().0.iter().map(|pair| pair[0]) {
        assert!(Arc::ptr_eq(&mapped[&row.labels.fingerprint()], &row.labels));
    }
    for index in 0..values.len() {
        let bytes = &values[index];
        let mut labels = lbls(&[crate::test_support::TestLabelPair::new("__name__", "up")]);
        labels.insert("raw", bytes.clone());
        let fp = labels.fingerprint();
        assert!(captured.labels[&fp].get_value("raw").unwrap().as_bytes() == bytes);
        assert!(mapped[&fp].get_value("raw").unwrap().as_bytes() == bytes);
        assert!(captured.samples.contains(&float_sample_row(
            fp,
            11_000,
            f64::from(u32::try_from(index).unwrap()) + 4.0,
            None
        )));
        let matcher = LabelMatcher::new("raw", MatchOp::Eq, bytes.clone());
        let selected_map = store
            .series_shared_by_fingerprint("tenant-a", std::slice::from_ref(&matcher), 9_000, 11_000)
            .await
            .unwrap();
        assert!(selected_map == BTreeMap::from([(fp, Arc::clone(&mapped[&fp]))]));
        let selected = store
            .try_latest_float_scan("tenant-a", &[matcher], 9_000, 9_001, 11_000, 2)
            .await
            .unwrap()
            .unwrap();
        assert!(
            selected.samples
                == vec![float_sample_row(
                    fp,
                    11_000,
                    f64::from(u32::try_from(index).unwrap()) + 4.0,
                    None
                )]
        );
        assert!(selected.labels.len() == 1 && selected.labels[&fp].as_ref() == &labels);
    }
    head.delete_tenant("tenant-a");
    assert!(head.snapshot().floats.is_empty());
    assert!(captured.labels.len() == 3 && snapshot.floats["tenant-a"].len() == 6);
}
