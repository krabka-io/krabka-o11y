use std::sync::Arc;

use super::*;

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
