use super::{
    BlockStoreError, ObjectPath, ObjectStore, TimeRange,
    read_tenant_log_index_shard_ranges_from_object_store,
};

/// Reads the shard catalog of `tenant`, treating an absent catalog as empty.
///
/// A tenant that has never had a catalog written is not a fault, so this
/// returns no ranges for it. A catalog that is present must still be
/// well-formed.
///
/// # Errors
/// Returns the block-store error for every failure except an absent catalog.
pub async fn read_tenant_log_index_shard_ranges_or_empty_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
) -> Result<Vec<TimeRange>, BlockStoreError> {
    match read_tenant_log_index_shard_ranges_from_object_store(store, prefix, tenant).await {
        Ok(shard_ranges) => Ok(shard_ranges),
        Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => Ok(Vec::new()),
        Err(error) => Err(error),
    }
}
