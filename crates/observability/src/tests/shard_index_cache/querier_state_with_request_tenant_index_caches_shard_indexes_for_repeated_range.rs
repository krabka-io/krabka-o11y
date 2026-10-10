use assert2::assert;

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
    let state = querier_state_over_index_shards(
        &store,
        TenantIndexShards {
            prefix: &prefix,
            tenant,
            shard_ranges: &[shard_range],
            labels_index: &labels_index,
            block_index: &block_index,
        },
    )
    .await;

    let first = state
        .with_request_tenant_index(tenant, query_range)
        .await
        .unwrap();
    let second = state
        .with_request_tenant_index(tenant, query_range)
        .await
        .unwrap();

    assert!(
        first.label_index.as_ref() == &labels_index && first.block_index.as_ref() == &block_index
    );
    assert!(
        second.label_index.as_ref() == &labels_index && second.block_index.as_ref() == &block_index
    );

    // A moving window misses the merged-request cache but still uses the same
    // tenant-filtered shard. It must retain the complete immutable snapshot.
    let moving = state
        .with_request_tenant_index(tenant, TimeRange::new(5, 105).unwrap())
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&first.label_index, &moving.label_index));
    assert!(Arc::ptr_eq(&first.block_index, &moving.block_index));
    state.dynamic_index_cache.clear();
    assert!(moving.label_index.as_ref() == &labels_index);
    assert!(moving.block_index.as_ref() == &block_index);

    let list_count = shard_prefix_list_count(&store, &prefix, tenant);
    let shard_get_count = shard_snapshot_get_count(
        &store,
        TenantIndexShard {
            prefix: &prefix,
            tenant,
            shard_range,
        },
    );

    assert!(list_count == 1, "shard prefix should be listed once");
    assert!(
        shard_get_count == 1,
        "shard snapshot should be fetched once"
    );
}
