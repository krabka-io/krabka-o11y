use super::{
    BTreeSet, MAX_REQUEST_TENANTS, RequestTenantError, TENANT_SEPARATOR, TenantId, TenantPolicy,
};

/// Resolves the one tenant that a metrics request names.
///
/// `value` is the raw `X-Scope-OrgID` header or gRPC metadata value, or `None`
/// when the request has neither. Each part of the value between `|`
/// separators goes through [`TenantId::resolve`], so the charset, length and
/// path-segment rules come only from `krabka-blockstore`.
///
/// Grafana Mimir reads the value the same way. It checks every part in header
/// order, and then it counts the distinct parts. `a|a` is the tenant `a`,
/// `a|b` is two tenants, and `a|b/c` is an invalid tenant `b/c`. An empty part
/// passes the check and counts as a tenant, so `a|` is two tenants.
///
/// A value that holds only empty parts, such as `|`, names no tenant. Mimir
/// answers a query with that value with a 500 `invalid tenant id`, and it
/// accepts a push under an empty tenant. A [`TenantId`] cannot be empty, so
/// Krabka answers `no org id`, as it does for an absent header.
///
/// # Errors
///
/// Returns [`RequestTenantError::Resolve`] when the value is absent, is empty,
/// or has a part that is not a valid tenant id. Returns
/// [`RequestTenantError::TooManyTenants`] when the value names more than
/// [`MAX_REQUEST_TENANTS`] distinct tenants.
pub fn resolve_request_tenant(value: Option<&[u8]>) -> Result<TenantId, RequestTenantError> {
    let mut named = BTreeSet::new();
    for part in value
        .unwrap_or_default()
        .split(|byte| *byte == TENANT_SEPARATOR)
    {
        let tenant = if part.is_empty() {
            None
        } else {
            Some(TenantId::resolve(Some(part), &TenantPolicy::Required)?)
        };
        named.insert(tenant);
    }
    if named.len() > MAX_REQUEST_TENANTS {
        return Err(RequestTenantError::TooManyTenants {
            actual: named.len(),
        });
    }
    match named.pop_first().flatten() {
        Some(tenant) => Ok(tenant),
        // Every part is empty, so the request names no tenant. The resolver
        // decides what that means under the required policy.
        None => Ok(TenantId::resolve(None, &TenantPolicy::Required)?),
    }
}
