use super::*;

#[tokio::test]
pub(crate) async fn querier_state_with_request_tenant_index_caches_shard_indexes_for_repeated_range()
 {
    let store = RecordingObjectStore::new();
    let prefix = ObjectPath::from("observability/logs");
    let tenant = "tenant-a";
    let query_range = TimeRange::new(0, 100).unwrap();
    let mut labels_index = LabelIndex::default();
    let api = labels_index.insert_series(tenant, krabka_blockstore::labels([("app", "api")]));
    let mut block_index = BlockIndex::default();
    let shard_range = TimeRange::new(10, 19).unwrap();
    block_index.insert(BlockDescriptor::new(
        BlockKey::new(tenant, 0, 42, 43, shard_range),
        BTreeSet::from([api]),
    ));
    krabka_blockstore::write_tenant_log_index_shards_to_object_store(
        &store,
        &prefix,
        tenant,
        &[shard_range],
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
        .with_request_tenant_index(tenant, query_range)
        .await
        .unwrap();
    let second = state
        .with_request_tenant_index(tenant, query_range)
        .await
        .unwrap();

    assert_eq!(
        first.label_index.label_names(tenant),
        BTreeSet::from(["app".to_string()])
    );
    assert_eq!(
        second.label_index.label_names(tenant),
        BTreeSet::from(["app".to_string()])
    );

    let shard_prefix =
        krabka_blockstore::log_tenant_index_shards_object_prefix(&prefix, tenant).to_string();
    let shard_manifest = krabka_blockstore::log_tenant_index_shard_manifest_object_path(
        &prefix,
        tenant,
        shard_range,
    )
    .to_string();
    let list_count = store
        .list_prefixes()
        .into_iter()
        .filter(|prefix| prefix == &shard_prefix)
        .count();
    let shard_get_count = store
        .get_paths()
        .into_iter()
        .filter(|path| path == &shard_manifest)
        .count();

    assert!(list_count == 1, "shard prefix should be listed once");
    assert!(
        shard_get_count == 1,
        "shard manifest should be fetched once"
    );
}
