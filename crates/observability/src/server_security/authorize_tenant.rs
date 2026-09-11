use krabka_blockstore::TenantId;

use super::{Principal, TenantDenied};

/// Checks that `principal` may use `tenant`.
///
/// Call it right after the signal resolves the request's tenant. An
/// [`Principal::Unauthenticated`] request always passes, as it does upstream.
/// An authenticated principal passes when its grant lists `tenant` or is `*`.
///
/// # Errors
///
/// Returns [`TenantDenied`] when the principal's grant does not include
/// `tenant`. The denial is also reported to the principal's
/// [`SecurityEvents`](super::SecurityEvents).
pub fn authorize_tenant(principal: &Principal, tenant: &TenantId) -> Result<(), TenantDenied> {
    match principal {
        Principal::Unauthenticated => Ok(()),
        Principal::Authenticated { tenants, .. } if tenants.allows(tenant) => Ok(()),
        Principal::Authenticated {
            name,
            method,
            events,
            ..
        } => {
            events.events().tenant_denied(name, *method, tenant);
            Err(TenantDenied {
                principal: name.clone(),
                tenant: tenant.clone(),
            })
        }
    }
}
