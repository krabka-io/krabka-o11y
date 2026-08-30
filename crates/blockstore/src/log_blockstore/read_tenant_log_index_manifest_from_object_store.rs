use super::*;

#[instrument(level = "debug", skip_all, fields(tenant = %tenant), err)]
/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub async fn read_tenant_log_index_manifest_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
) -> Result<(LabelIndex, BlockIndex), BlockStoreError> {
    let bytes = store
        .get(&log_tenant_index_manifest_object_path(prefix, tenant))
        .await?
        .bytes()
        .await?;
    let manifest: LogIndexManifest = serde_json::from_slice(&bytes)?;
    manifest.into_indexes_for_tenant(tenant)
}
