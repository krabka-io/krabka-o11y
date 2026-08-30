use super::*;

/// The shard catalog gains a compacted range once, and only once. Losing
/// the push leaves a shard nobody can find; losing the containment test
/// lists it twice, and the querier then reads the same shard twice.
#[tokio::test]
pub(crate) async fn the_shard_catalog_lists_each_compacted_range_exactly_once() {
    let store = RecordingObjectStore::new();
    let prefix = ObjectPath::from("observability");
    let tenant = "tenant-a";
    let range = TimeRange::new(300, 399).unwrap();
    let mut labels_index = LabelIndex::default();
    let api = labels_index.insert_series(tenant, BTreeMap::from([("app".into(), "api".into())]));
    let descriptor = BlockDescriptor::new(
        BlockKey::new(tenant, 0, 30, 39, range),
        BTreeSet::from([api]),
    );
    let mut block_index = BlockIndex::default();
    block_index.insert(descriptor.clone());

    for round in 1..=2 {
        write_tenant_compaction_indexes_to_object_store(
            &store,
            &prefix,
            tenant,
            &descriptor,
            &labels_index,
            &block_index,
            LogCompactionIndexOutput::FullManifestAndShardCatalog,
        )
        .await
        .unwrap();

        let catalog = read_tenant_log_index_shard_ranges_from_object_store(&store, &prefix, tenant)
            .await
            .unwrap();
        check!(catalog == vec![range], "after round {round}");
    }
}
