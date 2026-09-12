use super::{
    BlockStoreError, ObjectPath, ObjectStore, ObjectStoreExt, TimeRange, instrument,
    log_tenant_index_shard_manifest_object_path,
};

/// Deletes the manifest object of one index shard.
///
/// A shard that keeps no block names nothing, so its manifest is a small inert
/// object that no query needs and every listing pass reads. Retention deletes
/// it rather than rewrites it empty.
///
/// [`read_tenant_log_index_shards_from_object_store`] reads an absent shard
/// manifest as an empty shard, so a query that listed the shard before this
/// delete still succeeds.
///
/// An object that is already gone is not a fault. A sweep that was interrupted,
/// and a sweep that runs twice, both reach the same manifest.
///
/// [`read_tenant_log_index_shards_from_object_store`]: crate::read_tenant_log_index_shards_from_object_store
///
/// # Errors
/// Returns [`BlockStoreError::ObjectStore`] when the delete fails for a reason
/// other than an absent object.
#[instrument(
    skip_all,
    fields(tenant = %tenant, start_ns = shard_range.start_ns, end_ns = shard_range.end_ns),
    err
)]
pub async fn delete_tenant_log_index_shard_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    shard_range: TimeRange,
) -> Result<(), BlockStoreError> {
    match store
        .delete(&log_tenant_index_shard_manifest_object_path(
            prefix,
            tenant,
            shard_range,
        ))
        .await
    {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(error) => Err(error.into()),
    }
}
