use super::*;

pub(crate) async fn materialize_log_deletes_before_compaction(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    delete_requests: &SharedLogDeleteRequests,
    tenant_indexes: &mut TenantCompactionIndexCache,
) -> Result<(), CompactorRunError> {
    if !active_log_delete_tenants(delete_requests)?.is_empty() {
        materialize_delete_requests_in_existing_object_store_blocks(store, prefix, delete_requests)
            .await?;
        tenant_indexes.clear();
    }
    Ok(())
}
