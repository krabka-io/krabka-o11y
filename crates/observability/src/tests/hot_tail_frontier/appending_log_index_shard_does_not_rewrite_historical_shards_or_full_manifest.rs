use super::*;

#[tokio::test]
pub(crate) async fn appending_log_index_shard_does_not_rewrite_historical_shards_or_full_manifest()
{
    let store = RecordingObjectStore::new();
    let prefix = ObjectPath::from("observability");
    let tenant = "tenant-a";
    let old_range_a = TimeRange::new(100, 199).unwrap();
    let old_range_b = TimeRange::new(200, 299).unwrap();
    let new_range = TimeRange::new(300, 399).unwrap();
    let mut labels_index = LabelIndex::default();
    let api = labels_index.insert_series(tenant, BTreeMap::from([("app".into(), "api".into())]));
    let worker =
        labels_index.insert_series(tenant, BTreeMap::from([("app".into(), "worker".into())]));
    let admin =
        labels_index.insert_series(tenant, BTreeMap::from([("app".into(), "admin".into())]));
    let mut block_index = BlockIndex::default();
    block_index.insert(BlockDescriptor::new(
        BlockKey::new(tenant, 0, 10, 19, old_range_a),
        BTreeSet::from([api]),
    ));
    block_index.insert(BlockDescriptor::new(
        BlockKey::new(tenant, 0, 20, 29, old_range_b),
        BTreeSet::from([worker]),
    ));
    krabka_blockstore::write_tenant_log_index_shards_to_object_store(
        &store,
        &prefix,
        tenant,
        &[old_range_a, old_range_b],
        &labels_index,
        &block_index,
    )
    .await
    .unwrap();

    let new_descriptor = BlockDescriptor::new(
        BlockKey::new(tenant, 0, 30, 39, new_range),
        BTreeSet::from([admin]),
    );
    block_index.insert(new_descriptor.clone());
    store.clear_put_paths();

    write_tenant_compaction_indexes_to_object_store(
        &store,
        &prefix,
        tenant,
        &new_descriptor,
        &labels_index,
        &block_index,
        LogCompactionIndexOutput::ShardManifests,
    )
    .await
    .unwrap();

    // Exactly one PUT is allowed: the new shard manifest. The global
    // tenant manifest, the shard catalog, and the old shard manifests
    // must not be rewritten.
    let put_paths = store.put_paths();
    assert_eq!(
        put_paths,
        vec![
            krabka_blockstore::log_tenant_index_shard_manifest_object_path(
                &prefix, tenant, new_range
            )
            .to_string()
        ],
        "only the new shard manifest should be written"
    );
}
