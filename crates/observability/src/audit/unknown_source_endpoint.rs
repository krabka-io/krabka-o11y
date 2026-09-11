use std::net::Ipv4Addr;

use super::AuditEndpoint;

/// The network source for a request whose peer address the service does not
/// know: `0.0.0.0`, port `0`.
#[must_use]
pub fn unknown_source_endpoint() -> AuditEndpoint {
    AuditEndpoint {
        ip: Ipv4Addr::UNSPECIFIED.to_string(),
        port: 0,
    }
}
