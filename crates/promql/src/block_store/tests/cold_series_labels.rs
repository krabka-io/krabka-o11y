use std::time::Duration;

use krabka_blockstore::MatchOp;

use super::*;
use crate::{InMemoryMetricStore, MergedMetricStore, PromqlMatcher as LabelMatcher, WalHead};

#[tokio::test]
async fn cold_label_values_survive_the_merged_instant_scan() {
    let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let mut floats = BlockStore::new(Arc::clone(&backing), base.clone());
    let mut histograms = BlockStore::new(backing, base);
    let up = labels(&[("__name__", "up"), ("job", "api"), ("region", "東京")]);
    let down = labels(&[("__name__", "down"), ("job", "worker")]);
    let fp = up.fingerprint();
    // Index identities can differ from the canonical label fingerprint. The
    // returned labels still deduplicate and resolve by their canonical hash.
    floats.index_mut().add_series("t", fp ^ 1, &up);
    floats.index_mut().add_series("t", fp ^ 2, &up);
    floats
        .index_mut()
        .add_series("t", down.fingerprint(), &down);
    floats.index_mut().add_series("other", fp, &up);
    histograms.index_mut().add_series("t", fp ^ 3, &up);
    let cold = MetricBlockStore::with_histograms(floats, histograms);
    let matcher = [LabelMatcher::new("__name__", MatchOp::Eq, "up")];
    let first = cold.series_shared("t", &matcher, 0, 100).await.unwrap();
    let second = cold.series_shared("t", &matcher, 0, 100).await.unwrap();
    assert2::assert!(first.len() == 1 && first[0].as_ref() == &up);
    assert2::assert!(second.len() == 1 && second[0].as_ref() == &up);
    // The histogram index retains the same complete label set.
    let histogram = cold
        .histograms
        .as_ref()
        .unwrap()
        .index()
        .series(
            "t",
            &[krabka_blockstore::LabelMatcher::new(
                "__name__",
                MatchOp::Eq,
                "up",
            )],
        )
        .unwrap();
    assert2::assert!(histogram == vec![up.clone()]);

    let mut hot = InMemoryMetricStore::new();
    hot.push_float("t", up.clone(), 10, 3.0);
    hot.push_float("t", up.clone(), 20, 7.0);
    hot.push_float("other", up.clone(), 30, 99.0);
    let merged = MergedMetricStore::new(cold, WalHead::from_store(hot));
    let scan = merged
        .try_latest_float_scan("t", &matcher, 0, 1, 30, 2)
        .await
        .unwrap()
        .unwrap();
    assert2::assert!(scan.samples == vec![(fp, 20, 7.0, None)]);
    assert2::assert!(scan.labels.len() == 1 && scan.labels[&fp].as_ref() == &up);
    let held = Arc::downgrade(&scan.labels[&fp]);
    drop(merged);
    drop(first);
    drop(second);
    drop(histogram);
    assert2::assert!(held.upgrade().is_some());
    drop(scan);
    assert2::assert!(held.upgrade().is_none());
}

#[tokio::test]
async fn instant_cold_read_releases_hot_rows_and_keeps_captured_values() {
    let (backing, [started, resumed]) = CountingObjectStore::wrap_paused(Arc::new(InMemory::new()));
    let mut floats = BlockStore::new(backing, url::Url::parse("memory:///").unwrap());
    let up = labels(&[("__name__", "up"), ("job", "api")]);
    let fp = up.fingerprint();
    write_float_block(&mut floats, "cold.parquet", &up, 2_048, 20.0).await;

    let mut store = InMemoryMetricStore::new();
    for ts in 0..1_024 {
        store.push_float("tenant-a", up.clone(), ts, 10.0);
    }
    let head = WalHead::from_store(store);
    let rows = {
        let snapshot = head.snapshot();
        Arc::downgrade(snapshot.floats["tenant-a"].sealed_chunks().next().unwrap())
    };
    assert2::assert!(rows.upgrade().is_some());
    let merged = Arc::new(MergedMetricStore::new(
        MetricBlockStore::new(floats),
        head.clone(),
    ));
    let querying = Arc::clone(&merged);
    let query = tokio::spawn(async move {
        querying
            .try_latest_float_scan("tenant-a", &[], 0, 0, 8_192, 2_048)
            .await
            .unwrap()
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(30), started.notified())
        .await
        .unwrap();
    // Replace the current head while the captured query waits on cold I/O.
    head.update(|store| {
        store.delete_tenant("tenant-a");
        store.push_float("tenant-a", up.clone(), 4_096, 100.0);
    });
    assert2::assert!(rows.upgrade().is_none());
    resumed.notify_one();

    let captured = query.await.unwrap();
    let current = merged
        .try_latest_float_scan("tenant-a", &[], 0, 0, 8_192, 2_048)
        .await
        .unwrap()
        .unwrap();
    assert2::assert!(captured.samples == vec![(fp, 2_048, 20.0, None)]);
    assert2::assert!(current.samples == vec![(fp, 4_096, 100.0, None)]);
    let expected_labels = std::collections::BTreeMap::from([(fp, Arc::new(up.into()))]);
    assert2::assert!(captured.labels == expected_labels);
    assert2::assert!(current.labels == expected_labels);
}
