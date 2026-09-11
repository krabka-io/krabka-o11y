use super::TenantId;

/// Gives the tenant label for the ingest tracing span.
///
/// `tenant` is the result of `tenant_from_headers` for the request, so the
/// label is the name the WAL records carry. A request that does not resolve to
/// a tenant gets the fixed label `<unresolved>`. That label holds characters
/// that a tenant id cannot hold, so it never reads as a real tenant, and the
/// span never shows a name that the resolver rejected.
pub(crate) fn ingest_span_tenant(tenant: Option<&TenantId>) -> &str {
    tenant.map_or("<unresolved>", TenantId::as_str)
}
