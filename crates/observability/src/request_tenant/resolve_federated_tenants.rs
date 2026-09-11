use super::{BTreeSet, TenantId, TenantPolicy, TenantRequestError, TenantResolveError};

/// Resolves every tenant a federated read names, as `dskit`'s
/// `tenant.TenantIDs` does.
///
/// Loki runs one query across several tenants when the header lists them with
/// `|` and `querier.multi_tenant_queries_enabled` is on. Every part is checked
/// before the query runs, so one malformed part refuses the whole query. The
/// tenants come back sorted and without repeats, as `NormalizeTenantIDs`
/// leaves them. An empty part names no tenant and is dropped: Loki 3.5.1 with
/// multi-tenant queries enabled answers `a||b` and `a|` with the same result
/// as `a|b` and `a`.
///
/// # Errors
///
/// Returns [`TenantRequestError::Resolve`] when the header is absent or
/// empty, when every part is empty, or when a part is not a valid tenant id.
pub(crate) fn resolve_federated_tenants(
    value: Option<&[u8]>,
) -> Result<Vec<TenantId>, TenantRequestError> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Err(TenantResolveError::Missing.into());
    };
    let mut tenants = BTreeSet::new();
    for part in value.split(|byte| *byte == b'|') {
        if !part.is_empty() {
            tenants.insert(TenantId::resolve(Some(part), &TenantPolicy::Required)?);
        }
    }
    if tenants.is_empty() {
        return Err(TenantResolveError::Missing.into());
    }
    Ok(tenants.into_iter().collect())
}
