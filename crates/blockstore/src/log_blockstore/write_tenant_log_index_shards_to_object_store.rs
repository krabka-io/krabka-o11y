use super::{
    BlockIndex, BlockStoreError, LabelIndex, ObjectPath, ObjectStore, TimeRange,
    write_tenant_log_index_shard_catalog_to_object_store,
    write_tenant_log_index_shard_to_object_store,
};

/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn write_tenant_log_index_shards_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    shard_ranges: &[TimeRange],
    label_index: &LabelIndex,
    block_index: &BlockIndex,
) -> Result<(), BlockStoreError> {
    for shard_range in shard_ranges {
        write_tenant_log_index_shard_to_object_store(
            store,
            prefix,
            tenant,
            *shard_range,
            label_index,
            block_index,
        )
        .await?;
    }

    write_tenant_log_index_shard_catalog_to_object_store(store, prefix, tenant, shard_ranges).await
}
