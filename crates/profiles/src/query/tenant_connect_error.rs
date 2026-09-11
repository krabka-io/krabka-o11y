use super::{Code, ConnectError, TenantResolveError};

/// Maps a tenant that does not resolve to the Connect error the querier sends.
///
/// Pyroscope with multi-tenancy off does not read `X-Scope-OrgID`, so it has
/// no rejection to match here. Krabka isolates blocks and queries by tenant, so
/// it rejects a malformed name. The code is `invalid_argument`, because the
/// fault is in the request. The message is the text that Grafana's `dskit`
/// sends for the same header.
pub(crate) fn tenant_connect_error(error: &TenantResolveError) -> ConnectError {
    ConnectError::new(Code::InvalidArgument, error.to_string())
}
