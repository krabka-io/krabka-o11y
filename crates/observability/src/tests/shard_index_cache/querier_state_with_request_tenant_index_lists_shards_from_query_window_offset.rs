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
    let state = querier_state_over_index_shards(
        &store,
        TenantIndexShards {
            prefix: &prefix,
            tenant,
            shard_ranges: &[old_shard_range, matching_shard_range],
            labels_index: &labels_index,
            block_index: &block_index,
        },
    )
    .await;

    let state = state
        .with_request_tenant_index(tenant, query_range)
        .await
        .unwrap();

    let mut expected_blocks = BlockIndex::default();
    expected_blocks.insert(matching_block);
    assert!(state.label_index == labels_index && state.block_index == expected_blocks);

    let [old_shard_gets, matching_shard_gets] =
        [old_shard_range, matching_shard_range].map(|shard_range| {
            shard_snapshot_get_count(
                &store,
                TenantIndexShard {
                    prefix: &prefix,
                    tenant,
                    shard_range,
                },
            )
        });
    assert!(old_shard_gets == 0);
    assert!(matching_shard_gets == 1);
    assert!(
        shard_prefix_list_count(&store, &prefix, tenant) == 1,
        "list the complete tenant prefix once"
    );
    assert!(
        store.list_offsets().is_empty(),
        "no offset can prove overlap coverage"
    );
}
