use krabka_blockstore::ProfileWalRange;
use krabka_pprof::UnionProfileStore;

use super::*;
use crate::hot_store::WalTailProfileStore;

#[tokio::test]
async fn published_offsets_count_once_without_collapsing_identical_uploads() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let rec = record("t", "api", vec![0], 7);
    let hot = Arc::new(WalTailProfileStore::new());
    // Identical uploads at different offsets, a gap, and another partition
    // remain independent even though their timestamps and stacks are equal.
    hot.append_positioned_records([
        ((0, 0), rec.clone()),
        ((0, 1), rec.clone()),
        ((0, 2), rec.clone()),
        ((1, 0), rec.clone()),
    ])
    .unwrap();
    let cold = Arc::new(ColdProfileStore::new(
        Arc::clone(&store),
        Arc::new(ProfileIndex::new()),
    ));
    let union = Arc::new(UnionProfileStore::new(Arc::clone(&hot), Arc::clone(&cold)));
    let engine = FlameEngine::new(Arc::clone(&union), EngineOpts::default());
    let before = engine
        .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
        .await
        .unwrap();
    check!(before.total == 28);

    let mut index = ProfileIndex::new();
    let labels = Labels::from_pairs(rec.labels.iter().cloned());
    index
        .add_series("t", labels.fingerprint(), &labels)
        .unwrap();
    for offset in [0, 2] {
        let meta = build_block(
            &store,
            "t",
            0,
            std::slice::from_ref(&rec),
            (offset, offset),
            &krabka_blockstore::ObjectStoreMetrics::unregistered(),
        )
        .await
        .unwrap()
        .remove(0);
        index.add_block(&meta);
        index.add_profile_block("t", &meta.object_key, vec![STACKTRACE_PARTITION]);
        index.set_wal_ranges(
            &meta.object_key,
            vec![ProfileWalRange {
                partition: 0,
                min_offset: offset,
                max_offset: offset,
            }],
        );
    }
    let key = "profile-index/latest";
    index.save_latest_snapshot(&store, key).await.unwrap();
    let loaded = ProfileIndex::load_latest_snapshot(&store, key)
        .await
        .unwrap();
    cold.replace_index(Arc::new(loaded));
    let (pinned, ranges) = cold
        .select_with_source_ranges("t", PT, &[], 0, i64::MAX)
        .await
        .unwrap();
    check!(ranges.len() == 2);
    let after = engine
        .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
        .await
        .unwrap();
    check!(after == before);
    let legacy = FlameEngine::new(
        Arc::new(UnionProfileStore::new(
            hot.snapshot().unwrap(),
            Arc::clone(&cold),
        )),
        EngineOpts::default(),
    );
    let doubled = legacy
        .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
        .await
        .unwrap();
    check!(
        doubled.total == 42,
        "control without source coverage reproduces double counting"
    );

    let inputs = index
        .all_blocks()
        .into_iter()
        .map(|meta| meta.object_key)
        .collect::<Vec<_>>();
    let output = crate::compactor::compact_blocks(
        &store,
        &mut index,
        "t",
        &inputs,
        "profiles/t/compacted.parquet",
    )
    .await
    .unwrap();
    check!(index.wal_ranges(&output.object_key) == ranges);
    index.save_latest_snapshot(&store, key).await.unwrap();
    cold.replace_index(Arc::new(
        ProfileIndex::load_latest_snapshot(&store, key)
            .await
            .unwrap(),
    ));
    for input in inputs {
        store
            .delete(&Path::from(format!("{input}.symdb")))
            .await
            .unwrap();
        store.delete(&Path::from(input)).await.unwrap();
    }
    let compacted = engine
        .select_merge_stacktraces("t", PT, r#"{service_name="api"}"#, 0, i64::MAX, 0)
        .await
        .unwrap();
    check!(compacted == before);
    // The old cold scan carries its own coverage even after index refresh.
    let hot_scan = hot
        .select_excluding_source_ranges("t", PT, &[], (0, i64::MAX), &ranges)
        .await
        .unwrap();
    let batches = hot_scan
        .ctx
        .table(&hot_scan.samples_table)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    check!(batches.iter().map(RecordBatch::num_rows).sum::<usize>() == 2);
    let pinned_rows = pinned
        .ctx
        .table(&pinned.samples_table)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    check!(pinned_rows.iter().map(RecordBatch::num_rows).sum::<usize>() == 2);
}
