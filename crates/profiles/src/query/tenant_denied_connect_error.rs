use super::{Code, ConnectError, TenantDenied};

/// Maps a tenant that the principal of the request may not use to the Connect error the querier sends.
///
/// The code is `permission_denied`, which Connect sends with status 403. That
/// is the status of the plain HTTP routes for the same denial. The message is
/// the text of the denial. It names the principal and the tenant, and not the
/// tenants that the principal may use.
pub(crate) fn tenant_denied_connect_error(denied: &TenantDenied) -> ConnectError {
    ConnectError::new(Code::PermissionDenied, denied.to_string())
}
