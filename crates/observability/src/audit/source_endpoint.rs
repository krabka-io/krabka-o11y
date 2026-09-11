use std::net::SocketAddr;

use super::AuditEndpoint;

/// The network source of a request that arrived from `address`.
///
/// An IPv4 address in IPv6 form, such as `::ffff:10.0.0.1`, becomes its IPv4
/// form, so one client has one address in the trail.
#[must_use]
pub fn source_endpoint(address: SocketAddr) -> AuditEndpoint {
    AuditEndpoint {
        ip: address.ip().to_canonical().to_string(),
        port: address.port(),
    }
}
