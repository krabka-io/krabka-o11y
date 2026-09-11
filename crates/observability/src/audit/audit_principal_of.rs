use super::{AuditPrincipal, mechanism_of, principal, unauthenticated_principal};
use crate::server_security::Principal;

/// The audit principal for the principal the authentication layer put on a request.
///
/// An unauthenticated request becomes [`unauthenticated_principal`], so an
/// audit record for a server without a credentials file names no principal
/// that a credentials file could define.
#[must_use]
pub fn audit_principal_of(request_principal: &Principal) -> AuditPrincipal {
    match request_principal {
        Principal::Unauthenticated => unauthenticated_principal(),
        Principal::Authenticated { name, method, .. } => {
            principal(name.as_ref(), mechanism_of(*method))
        }
    }
}
