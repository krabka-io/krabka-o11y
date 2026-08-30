use super::*;

#[tokio::test]
pub(crate) async fn querier_state_with_request_tenant_index_lists_shards_from_query_window_offset()
{
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
    block_index.insert(BlockDescriptor::new(
        BlockKey::new(tenant, 0, 42, 43, matching_shard_range),
        BTreeSet::from([api]),
    ));
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

    assert_eq!(
        state.label_index.label_names(tenant),
        BTreeSet::from(["app".to_string()])
    );
    let expected_offset = krabka_blockstore::log_tenant_index_shards_object_prefix(&prefix, tenant)
        .join(format!("time={}", query_start - (query_end - query_start)))
        .to_string();
    assert!(
        store.list_offsets().contains(&expected_offset),
        "shard listing should start near the query window; offsets={:?}",
        store.list_offsets()
    );
}
