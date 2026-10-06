use std::{collections::BTreeMap, sync::Arc};

use assert2::assert;
use krabka_blockstore::BlockStore;
use object_store::memory::InMemory;

use super::*;
use crate::{MergedMetricStore, MetricBlockStore};

#[test]
fn labels_are_shared_across_records_and_sample_types_within_one_tenant() {
    let mut store = InMemoryMetricStore::new();
    let labels = lbls(&[("__name__", "up"), ("job", "api")]);
    store.push_float("t", labels.clone(), 100, 1.0);
    store.push_float("t", labels.clone(), 200, 2.0);
    store.push_histogram("t", labels.clone(), 300, native_histogram());
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
    store.push_float("t", lbls(&[("__name__", "up")]), 100, 1.0);
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
    let labels = lbls(&[("__name__", "up"), ("job", "api")]);
    let fp = labels.fingerprint();
    let mut store = InMemoryMetricStore::new();
    store.push_float("t", labels.clone(), 100, 1.0);
    let snapshot = store.clone();
    store.push_float("t", labels.clone(), 200, 2.0);
    store.push_histogram("t", labels.clone(), 300, native_histogram());
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

    let dead = Arc::new(labels.clone());
    Arc::make_mut(store.series_labels.get_mut("t").unwrap())
        .get_mut(&fp)
        .unwrap()
        .push(Arc::downgrade(&dead));
    drop(dead);
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
    let wanted = lbls(&[("__name__", "up")]);
    let other = Arc::new(lbls(&[("__name__", "down")]));
    let mut store = InMemoryMetricStore::new();
    // Force a hash collision at the cache seam; the equality check must still
    // distinguish the label sets without relying on finding a real collision.
    store.series_labels.insert(
        "t".into(),
        Arc::new(HashMap::from([(
            wanted.fingerprint(),
            vec![Arc::downgrade(&other)],
        )])),
    );
    store.push_float("t", wanted.clone(), 100, 1.0);
    let row = store.floats["t"].iter().next().unwrap();
    check!(row.labels.as_ref() == &wanted);
    check!(!Arc::ptr_eq(&row.labels, &other));
}

#[tokio::test]
async fn equal_fingerprints_do_not_reuse_matchers_for_different_labels() {
    let wanted = Arc::new(Labels::from_pairs([("__name__", "up"), ("job", "api")]));
    let other = Arc::new(Labels::from_pairs([("__name__", "down"), ("job", "api")]));
    let fp = wanted.fingerprint();
    let mut hot = InMemoryMetricStore::new();
    // Force collisions at the row seam. Reusing the last successful matcher
    // by fingerprint alone would incorrectly select the newer down sample.
    for (labels, stamp, value) in [
        (Arc::clone(&wanted), 10_000, 3.0),
        (Arc::new(wanted.as_ref().clone()), 11_000, 7.0),
        (other, 12_000, 99.0),
    ] {
        hot.floats
            .entry("tenant-a".into())
            .or_default()
            .push(FloatRow {
                fp,
                labels,
                ts_ms: stamp,
                value,
                start_timestamp_ms: None,
            });
    }
    let blocks = BlockStore::new(
        Arc::new(InMemory::new()),
        url::Url::parse("memory:///").unwrap(),
    );
    let store = MergedMetricStore::new(MetricBlockStore::new(blocks), WalHead::from_store(hot));
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
    assert!(scan.samples == vec![(fp, 11_000, 7.0, None)]);
    assert!(scan.labels.len() == 1);
    assert!(scan.labels[&fp].as_ref() == wanted.as_ref());
    assert!(Arc::ptr_eq(&scan.labels[&fp], &wanted));
}

#[tokio::test]
async fn cold_labels_do_not_hide_different_hot_labels_with_the_same_row_id() {
    let cold_labels = Arc::new(Labels::from_pairs([("__name__", "up"), ("job", "api")]));
    let hot_labels = Arc::new(Labels::from_pairs([("__name__", "up"), ("job", "worker")]));
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
    let store = MergedMetricStore::new(MetricBlockStore::new(blocks), WalHead::from_store(hot));
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
    assert!(scan.samples == vec![(cold_fp, 10_000, 7.0, None)]);
    assert!(
        scan.labels == BTreeMap::from([(cold_fp, Arc::clone(&cold_labels)), (hot_fp, hot_labels),])
    );
    assert!(!Arc::ptr_eq(&scan.labels[&cold_fp], &cold_labels));
}

type SummaryLedger = BTreeMap<u64, (Arc<Labels>, i64, u64, Option<i64>)>;

pub(super) fn summary_ledger(
    store: &InMemoryMetricStore,
    tenant: &str,
    matchers: &[LabelMatcher],
    label_start_ms: i64,
    sample_start_ms: i64,
    end_ms: i64,
) -> Option<(SummaryLedger, usize)> {
    let prepared = prepare_matchers(matchers).unwrap();
    let (series, count) = store.float_head_summaries.get(tenant)?.matching_series(
        store.floats.get(tenant)?.len(),
        &prepared,
        label_start_ms,
        sample_start_ms,
        end_ms,
    )?;
    Some((
        series
            .into_iter()
            .map(|series| {
                (
                    series.latest.0,
                    (
                        Arc::clone(&series.labels),
                        series.latest.1,
                        series.latest.2.to_bits(),
                        series.latest.3,
                    ),
                )
            })
            .collect(),
        count,
    ))
}

#[tokio::test]
async fn eligible_summary_keeps_old_rows_boundaries_raw_counts_and_future_fallback() {
    let labels = lbls(&[("__name__", "up"), ("job", "api")]);
    let boundary = lbls(&[("__name__", "up"), ("job", "boundary")]);
    let histogram = lbls(&[("__name__", "up"), ("job", "histogram")]);
    let mut hot = InMemoryMetricStore::new();
    for (stamp, value, start) in [
        (1_000, 1.0, None),
        (9_000, 3.0, None),
        (11_000, 7.0, Some(0)),
        (11_000, 99.0, None),
        (10_000, 4.0, None),
    ] {
        hot.push_float_with_start_timestamp("t", labels.clone(), stamp, value, start);
    }
    hot.push_float("t", boundary.clone(), 8_000, 40.0);
    hot.push_float("t", boundary.clone(), 9_000, 42.0);
    hot.push_histogram("t", histogram.clone(), 9_000, native_histogram());
    hot.push_float("t", lbls(&[("__name__", "down")]), 13_000, 99.0);
    hot.push_float("other", labels.clone(), 11_000, 99.0);
    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let expected = BTreeMap::from([
        (
            labels.fingerprint(),
            (Arc::new(labels.clone()), 11_000, 7.0_f64.to_bits(), Some(0)),
        ),
        (
            boundary.fingerprint(),
            (Arc::new(boundary.clone()), 9_000, 42.0_f64.to_bits(), None),
        ),
    ]);
    // This positive assertion cannot be satisfied by the ordinary row fallback.
    // Old retained rows count conservatively; labels-only rows contribute zero.
    assert!(summary_ledger(&hot, "t", &matchers, 9_000, 9_001, 12_000) == Some((expected, 5)));
    let head = WalHead::from_store(hot);
    let blocks = BlockStore::new(
        Arc::new(InMemory::new()),
        url::Url::parse("memory:///").unwrap(),
    );
    let store = MergedMetricStore::new(MetricBlockStore::new(blocks), head.clone());
    let scan = store
        .try_latest_float_scan("t", &matchers, 9_000, 9_001, 12_000, 5)
        .await
        .unwrap()
        .unwrap();
    assert!(scan.samples == vec![(labels.fingerprint(), 11_000, 7.0, Some(0))]);
    assert!(
        scan.labels
            == [labels.clone(), boundary, histogram]
                .into_iter()
                .map(|labels| (labels.fingerprint(), Arc::new(labels)))
                .collect::<BTreeMap<_, _>>()
    );
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
    let finite = lbls(&[("__name__", "up"), ("job", "finite")]);
    let stale = lbls(&[("__name__", "up"), ("job", "stale")]);
    let zero = lbls(&[("__name__", "up"), ("job", "zero")]);
    let mut hot = InMemoryMetricStore::new();
    for (labels, value, start) in [
        (finite.clone(), 7.0, Some(0)),
        (finite.clone(), 99.0, None),
        (
            stale.clone(),
            f64::from_bits(0x7ff0_0000_0000_0002),
            Some(0),
        ),
        (stale.clone(), 99.0, None),
        (zero.clone(), -0.0, None),
        (zero.clone(), 99.0, Some(0)),
    ] {
        hot.push_float_with_start_timestamp("t", labels, 11_000, value, start);
    }
    let matchers = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let expected = BTreeMap::from([
        (
            finite.fingerprint(),
            (Arc::new(finite.clone()), 11_000, 7.0_f64.to_bits(), Some(0)),
        ),
        (
            stale.fingerprint(),
            (
                Arc::new(stale.clone()),
                11_000,
                0x7ff0_0000_0000_0002,
                Some(0),
            ),
        ),
        (
            zero.fingerprint(),
            (Arc::new(zero.clone()), 11_000, (-0.0_f64).to_bits(), None),
        ),
    ]);
    assert!(summary_ledger(&hot, "t", &matchers, 9_000, 9_001, 12_000) == Some((expected, 6)));
    let blocks = BlockStore::new(
        Arc::new(InMemory::new()),
        url::Url::parse("memory:///").unwrap(),
    );
    let store = MergedMetricStore::new(MetricBlockStore::new(blocks), WalHead::from_store(hot));
    let scan = store
        .try_latest_float_scan("t", &matchers, 9_000, 9_001, 12_000, 6)
        .await
        .unwrap()
        .unwrap();
    let mut expected_samples = vec![
        (finite.fingerprint(), 11_000, 7.0_f64.to_bits(), Some(0)),
        (stale.fingerprint(), 11_000, 0x7ff0_0000_0000_0002, Some(0)),
        (zero.fingerprint(), 11_000, (-0.0_f64).to_bits(), None),
    ];
    expected_samples.sort_by_key(|row| row.0);
    assert!(
        scan.samples
            .iter()
            .map(|row| (row.0, row.1, row.2.to_bits(), row.3))
            .collect::<Vec<_>>()
            == expected_samples
    );
    assert!(
        scan.labels
            == [finite, stale, zero]
                .into_iter()
                .map(|labels| (labels.fingerprint(), Arc::new(labels)))
                .collect::<BTreeMap<_, _>>()
    );
}

#[tokio::test]
async fn summary_falls_back_after_direct_append_and_rebuilt_label_identity_collision() {
    let wanted = lbls(&[("__name__", "up"), ("job", "api")]);
    let other = Arc::new(lbls(&[("__name__", "down"), ("job", "api")]));
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
    assert!(hot.prune(10_000).samples_dropped == 1);
    // Pruning rebuilds a complete summary; the two distinct label Arcs still
    // require the row path, including the rejected newer 'down' sample.
    assert!(summary_ledger(&hot, "t", &matchers, 9_000, 9_001, 12_000).is_none());
    let blocks = BlockStore::new(
        Arc::new(InMemory::new()),
        url::Url::parse("memory:///").unwrap(),
    );
    let store = MergedMetricStore::new(MetricBlockStore::new(blocks), WalHead::from_store(hot));
    let scan = store
        .try_latest_float_scan("t", &matchers, 9_000, 9_001, 12_000, 2)
        .await
        .unwrap()
        .unwrap();
    assert!(scan.samples == vec![(fp, 11_000, 7.0, Some(0))]);
    assert!(scan.labels == BTreeMap::from([(fp, Arc::new(wanted))]));
    assert!(Arc::ptr_eq(&scan.labels[&fp], &wanted_arc));

    let wanted = lbls(&[("__name__", "up"), ("job", "api")]);
    let in_window = Arc::new(wanted.clone());
    let mut hot = InMemoryMetricStore::with_retention(secs(3));
    hot.push_float("t", wanted, 8_000, 1.0);
    hot.push_float("other", lbls(&[("__name__", "expired")]), 100, 1.0);
    hot.floats.get_mut("t").unwrap().push(FloatRow {
        fp,
        labels: Arc::clone(&in_window),
        ts_ms: 11_000,
        value: 7.0,
        start_timestamp_ms: Some(0),
    });
    assert!(hot.prune(10_000).samples_dropped == 1);
    assert!(summary_ledger(&hot, "t", &matchers, 9_000, 9_001, 12_000).is_none());
    let blocks = BlockStore::new(
        Arc::new(InMemory::new()),
        url::Url::parse("memory:///").unwrap(),
    );
    let store = MergedMetricStore::new(MetricBlockStore::new(blocks), WalHead::from_store(hot));
    let scan = store
        .try_latest_float_scan("t", &matchers, 9_000, 9_001, 12_000, 2)
        .await
        .unwrap()
        .unwrap();
    assert!(scan.samples == vec![(fp, 11_000, 7.0, Some(0))]);
    assert!(scan.labels == BTreeMap::from([(fp, Arc::clone(&in_window))]));
    assert!(Arc::ptr_eq(&scan.labels[&fp], &in_window));
}

#[tokio::test]
async fn byte_labels_remain_distinct_in_interning_and_latest_scan_snapshots() {
    let values = [vec![0xff], vec![0xfe], "\u{fffd}".as_bytes().to_vec()];
    let mut hot = InMemoryMetricStore::new();
    for (index, bytes) in values.iter().enumerate() {
        let mut labels = lbls(&[("__name__", "up")]);
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
    let expected = values
        .iter()
        .zip([4.0_f64, 5.0, 6.0])
        .map(|(bytes, value)| {
            let mut labels = lbls(&[("__name__", "up")]);
            labels.insert("raw", bytes.clone());
            (
                labels.fingerprint(),
                (Arc::new(labels), 11_000, value.to_bits(), None),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert!(summary_ledger(&hot, "tenant-a", &[], 9_000, 9_001, 11_000) == Some((expected, 6)));
    let head = WalHead::from_store(hot);
    let snapshot = head.snapshot();
    let rows = snapshot.floats["tenant-a"].iter().collect::<Vec<_>>();
    for pair in rows.as_chunks::<2>().0 {
        assert!(Arc::ptr_eq(&pair[0].labels, &pair[1].labels));
    }
    assert!(snapshot.series_labels["tenant-a"].len() == 3);
    let blocks = BlockStore::new(
        Arc::new(InMemory::new()),
        url::Url::parse("memory:///").unwrap(),
    );
    let store = MergedMetricStore::new(MetricBlockStore::new(blocks), head.clone());
    let captured = store
        .try_latest_float_scan("tenant-a", &[], 9_000, 9_001, 11_000, 6)
        .await
        .unwrap()
        .unwrap();
    assert!(captured.samples.len() == 3 && captured.labels.len() == 3);
    for (index, bytes) in values.iter().enumerate() {
        let mut labels = lbls(&[("__name__", "up")]);
        labels.insert("raw", bytes.clone());
        let fp = labels.fingerprint();
        assert!(captured.labels[&fp].get_value("raw").unwrap().as_bytes() == bytes);
        assert!(captured.samples.contains(&(
            fp,
            11_000,
            f64::from(u32::try_from(index).unwrap()) + 4.0,
            None
        )));
        let matcher = LabelMatcher::new("raw", MatchOp::Eq, bytes.clone());
        let selected = store
            .try_latest_float_scan("tenant-a", &[matcher], 9_000, 9_001, 11_000, 2)
            .await
            .unwrap()
            .unwrap();
        assert!(
            selected.samples
                == vec![(
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
