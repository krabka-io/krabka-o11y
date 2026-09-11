use std::net::SocketAddr;

use super::ServerSecurity;

/// Logs one warning that names the parts of `security` that are off.
///
/// An upstream component with no TLS and no authentication says nothing about
/// it. Krabka keeps the same default, and makes the default visible in the log.
pub fn warn_about_posture(local_addr: SocketAddr, security: &ServerSecurity) {
    match (security.tls().is_some(), security.authentication_enabled()) {
        (false, false) => tracing::warn!(
            %local_addr,
            "listener serves plain HTTP without authentication; set --server-tls-cert-path and --auth-credentials-config to secure it"
        ),
        (true, false) => tracing::warn!(
            %local_addr,
            "listener serves TLS without authentication; set --auth-credentials-config to require credentials"
        ),
        (false, true) => tracing::warn!(
            %local_addr,
            "listener authenticates requests over plain HTTP, so credentials cross the network unencrypted; set --server-tls-cert-path"
        ),
        (true, true) => {}
    }
}
