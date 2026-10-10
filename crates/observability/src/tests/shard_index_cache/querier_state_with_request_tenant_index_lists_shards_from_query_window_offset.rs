use assert2::assert;

use super::*;

#[tokio::test]
pub(crate) async fn querier_state_lists_full_shard_prefix_and_filters_before_fetch() {
    let store = RecordingObjectStore::new();
    let prefix = ObjectPath::from("observability/logs");
    let tenant = "tenant-a";
    let query_start = 1_700_000_000_000_000_000;
    let query_end = query_start + 300_000_000_000;
    let query_range = TimeRange::new(query_start, query_end).unwrap();
    let old_shard_range =
        TimeRange::new(query_start - 600_000_000_000, query_start - 599_000_000_000).unwrap();
    let matching_shard_range = TimeRange::new(query_start + 10, query_start + 20).unwrap();

    let mut labels_index = LabelIndex::default();
    let api = labels_index.insert_series(tenant, krabka_blockstore::labels([("app", "api")]));
    let mut block_index = BlockIndex::default();
    block_index.insert(BlockDescriptor::new(
        BlockKey::new(tenant, 0, 40, 41, old_shard_range),
        BTreeSet::from([api]),
    ));
    let matching_block = BlockDescriptor::new(
        BlockKey::new(tenant, 0, 42, 43, matching_shard_range),
        BTreeSet::from([api]),
    );
    block_index.insert(matching_block.clone());
    krabka_blockstore::write_tenant_log_index_shards_to_object_store(
        &store,
        &prefix,
        tenant,
        &[old_shard_range, matching_shard_range],
        &labels_index,
        &block_index,
    )
    .await
    .unwrap();
    store.clear_recorded_paths();

    let state = QuerierState::new(
        tempfile::tempdir().unwrap().keep(),
        LabelIndex::default(),
        BlockIndex::default(),
    )
    .with_dynamic_tenant_object_store_shards(Arc::new(store.clone()), prefix.clone());

    let state = state
        .with_request_tenant_index(tenant, query_range)
        .await
        .unwrap();

    let mut expected_blocks = BlockIndex::default();
    expected_blocks.insert(matching_block);
    assert!(
        state.label_index.as_ref() == &labels_index
            && state.block_index.as_ref() == &expected_blocks
    );

    let shard_prefix =
        krabka_blockstore::log_tenant_index_shards_object_prefix(&prefix, tenant).to_string();
    let snapshot_paths = [old_shard_range, matching_shard_range].map(|range| {
        let key =
            krabka_blockstore::log_tenant_index_shard_manifest_object_path(&prefix, tenant, range);
        format!(
            "{}/00000000000000000000.json",
            krabka_blockstore::index_snapshot_prefix_for_key(key.as_ref())
        )
    });
    let gets = store.get_paths();
    assert!(
        gets.iter()
            .filter(|path| *path == &snapshot_paths[0])
            .count()
            == 0
    );
    assert!(
        gets.iter()
            .filter(|path| *path == &snapshot_paths[1])
            .count()
            == 1
    );
    assert!(
        store
            .list_prefixes()
            .iter()
            .filter(|prefix| *prefix == &shard_prefix)
            .count()
            == 1,
        "list the complete tenant prefix once"
    );
    assert!(
        store.list_offsets().is_empty(),
        "no offset can prove overlap coverage"
    );
}
