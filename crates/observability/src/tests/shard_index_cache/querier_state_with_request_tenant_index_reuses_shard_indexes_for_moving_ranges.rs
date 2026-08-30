use super::*;

#[tokio::test]
pub(crate) async fn querier_state_with_request_tenant_index_reuses_shard_indexes_for_moving_ranges()
{
    let store = RecordingObjectStore::new();
    let prefix = ObjectPath::from("observability/logs");
    let tenant = "tenant-a";
    let first_query_range = TimeRange::new(0, 100).unwrap();
    let moving_query_range = TimeRange::new(5, 105).unwrap();
    let shard_range_a = TimeRange::new(10, 19).unwrap();
    let shard_range_b = TimeRange::new(80, 89).unwrap();

    let mut labels_index = LabelIndex::default();
    let api = labels_index.insert_series(tenant, krabka_blockstore::labels([("app", "api")]));
    let worker = labels_index.insert_series(tenant, krabka_blockstore::labels([("app", "worker")]));
    let mut block_index = BlockIndex::default();
    block_index.insert(BlockDescriptor::new(
        BlockKey::new(tenant, 0, 42, 43, shard_range_a),
        BTreeSet::from([api]),
    ));
    block_index.insert(BlockDescriptor::new(
        BlockKey::new(tenant, 0, 44, 45, shard_range_b),
        BTreeSet::from([worker]),
    ));
    krabka_blockstore::write_tenant_log_index_shards_to_object_store(
        &store,
        &prefix,
        tenant,
        &[shard_range_a, shard_range_b],
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

    let first = state
        .with_request_tenant_index(tenant, first_query_range)
        .await
        .unwrap();
    let second = state
        .with_request_tenant_index(tenant, moving_query_range)
        .await
        .unwrap();

    for state in [&first, &second] {
        check!(state.label_index.label_names(tenant) == BTreeSet::from(["app".to_string()]));
        check!(state.block_index.blocks().len() == 2);
    }

    let shard_prefix =
        krabka_blockstore::log_tenant_index_shards_object_prefix(&prefix, tenant).to_string();
    let shard_manifest_a = krabka_blockstore::log_tenant_index_shard_manifest_object_path(
        &prefix,
        tenant,
        shard_range_a,
    )
    .to_string();
    let shard_manifest_b = krabka_blockstore::log_tenant_index_shard_manifest_object_path(
        &prefix,
        tenant,
        shard_range_b,
    )
    .to_string();
    let list_count = store
        .list_prefixes()
        .into_iter()
        .filter(|prefix| prefix == &shard_prefix)
        .count();
    let shard_get_count_a = store
        .get_paths()
        .into_iter()
        .filter(|path| path == &shard_manifest_a)
        .count();
    let shard_get_count_b = store
        .get_paths()
        .into_iter()
        .filter(|path| path == &shard_manifest_b)
        .count();

    check!(list_count == 1, "shard prefix should be listed once");
    check!(
        shard_get_count_a == 1,
        "shard manifest A should be fetched once"
    );
    check!(
        shard_get_count_b == 1,
        "shard manifest B should be fetched once"
    );
}
