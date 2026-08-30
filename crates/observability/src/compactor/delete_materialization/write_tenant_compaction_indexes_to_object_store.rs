use super::*;

pub(crate) async fn write_tenant_compaction_indexes_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    new_descriptor: &BlockDescriptor,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
    index_output: LogCompactionIndexOutput,
) -> Result<(), BlockStoreError> {
    if index_output == LogCompactionIndexOutput::ShardManifests {
        let mut shard_block_index = BlockIndex::default();
        shard_block_index.insert(new_descriptor.clone());
        write_tenant_log_index_shard_to_object_store(
            store,
            prefix,
            tenant,
            new_descriptor.key.time_range,
            label_index,
            &shard_block_index,
        )
        .await?;
        return Ok(());
    }

    if index_output == LogCompactionIndexOutput::FullManifestAndShardCatalog {
        write_tenant_log_index_manifest_to_object_store(
            store,
            prefix,
            tenant,
            label_index,
            block_index,
        )
        .await?;
    }

    write_tenant_log_index_shard_to_object_store(
        store,
        prefix,
        tenant,
        new_descriptor.key.time_range,
        label_index,
        block_index,
    )
    .await?;

    let mut shard_ranges =
        match read_tenant_log_index_shard_ranges_from_object_store(store, prefix, tenant).await {
            Ok(shard_ranges) => shard_ranges,
            Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => Vec::new(),
            Err(error) => return Err(error),
        };
    if !shard_ranges.contains(&new_descriptor.key.time_range) {
        shard_ranges.push(new_descriptor.key.time_range);
    }
    shard_ranges.sort_by_key(|range| (range.start_ns, range.end_ns));
    write_tenant_log_index_shard_catalog_to_object_store(store, prefix, tenant, &shard_ranges).await
}
