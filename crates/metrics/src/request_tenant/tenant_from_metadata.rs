use super::{
    MetadataMap, MetadataValue, RequestTenantError, TENANT_HEADER, TenantId, resolve_request_tenant,
};

/// Resolves the tenant of a gRPC request from its `x-scope-orgid` metadata.
///
/// # Errors
///
/// Returns the errors of [`resolve_request_tenant`].
pub fn tenant_from_metadata(metadata: &MetadataMap) -> Result<TenantId, RequestTenantError> {
    resolve_request_tenant(metadata.get(TENANT_HEADER).map(MetadataValue::as_bytes))
}
