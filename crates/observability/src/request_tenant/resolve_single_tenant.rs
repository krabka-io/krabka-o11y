use super::{BTreeSet, TenantId, TenantPolicy, TenantRequestError, TenantResolveError};

/// Resolves the one tenant a request names, as `dskit`'s `tenant.TenantID`
/// does.
///
/// `dskit` splits the header on `|` and checks every part before it counts the
/// parts. A malformed part therefore wins over a count, and a part that
/// repeats counts once. An empty part is not checked, but it counts as a
/// distinct part, so `a|` names two tenants. Loki 3.5.1 answers a push with
/// each of those errors, and a push with `a|a` goes to tenant `a`.
///
/// # Errors
///
/// Returns [`TenantRequestError::Resolve`] when the header is absent or
/// empty, when every part is empty, or when a part is not a valid tenant id.
/// Returns [`TenantRequestError::MultipleOrgIds`] when the valid parts name
/// more than one distinct tenant.
pub(crate) fn resolve_single_tenant(value: Option<&[u8]>) -> Result<TenantId, TenantRequestError> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Err(TenantResolveError::Missing.into());
    };
    let mut parts = BTreeSet::new();
    let mut tenant = None;
    for part in value.split(|byte| *byte == b'|') {
        if !part.is_empty() {
            tenant = Some(TenantId::resolve(Some(part), &TenantPolicy::Required)?);
        }
        parts.insert(part);
    }
    if parts.len() > 1 {
        return Err(TenantRequestError::MultipleOrgIds);
    }
    // Krabka cannot hold an empty tenant, so a header of separators alone
    // names none. Upstream would serve it as the empty tenant.
    tenant.ok_or_else(|| TenantResolveError::Missing.into())
}
