use super::{AuditPrincipal, MECHANISM_NONE};

/// The principal name for a request that carries no credentials.
///
/// The angle brackets keep this name apart from a user name. A credentials
/// file should not define a user with this name.
pub const UNAUTHENTICATED_PRINCIPAL_NAME: &str = "<unauthenticated>";

/// The principal of a request that carries no credentials.
///
/// An event for such a request should also name the tenant from the request
/// as a resource, because the principal does not identify the caller.
#[must_use]
pub fn unauthenticated_principal() -> AuditPrincipal {
    AuditPrincipal {
        name: UNAUTHENTICATED_PRINCIPAL_NAME.to_owned(),
        auth_method: MECHANISM_NONE.to_owned(),
    }
}
