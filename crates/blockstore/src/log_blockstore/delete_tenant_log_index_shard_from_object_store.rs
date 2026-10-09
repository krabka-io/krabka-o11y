use super::{
    BlockStoreError, ObjectPath, ObjectStore, ObjectStoreExt, TimeRange, instrument,
    log_snapshot_error, log_tenant_index_shard_manifest_object_path,
};
use crate::index_snapshot::list_index_snapshot_objects;

/// Deletes the retained generations of one index shard.
///
/// This call does not coordinate concurrent publication. Callers should stop
/// the shard's publishers before deletion.
/// Retention keeps empty manifests instead, because deleting generation keys
/// while a writer is active can remove its publication.
///
/// # Errors
/// Returns an error when listing or deletion fails, except an already absent object.
#[instrument(skip_all, err)]
pub async fn delete_tenant_log_index_shard_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    shard_range: TimeRange,
) -> Result<(), BlockStoreError> {
    let key = log_tenant_index_shard_manifest_object_path(prefix, tenant, shard_range);
    for meta in list_index_snapshot_objects(store, key.as_ref())
        .await
        .map_err(log_snapshot_error)?
    {
        match store.delete(&meta.location).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
