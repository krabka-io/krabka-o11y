use super::ServerSecurity;

/// Refuses `--target all` with authentication on and no internal client.
///
/// Under `--target all` the query-frontend calls the querier, and the querier
/// calls the live-store, on loopback ports that serve with the same security
/// as every other listener. With authentication on and no
/// `--internal-client-*` flag, each of those calls is a 401, and every query
/// fails. The process stops at start instead.
///
/// # Errors
/// Returns an error that names the missing flags when `security` turns on
/// authentication and configures no internal client.
pub(crate) fn require_internal_credential(
    security: &ServerSecurity,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if security.authentication_enabled() && !security.internal_client().is_configured() {
        return Err("--target all with --auth-credentials-config needs --internal-client-token-path or --internal-client-tls-cert-path: the query-frontend and the querier call the roles beside them through authenticated listeners".into());
    }
    Ok(())
}
