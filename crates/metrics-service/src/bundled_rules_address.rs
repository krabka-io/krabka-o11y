use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// The address that the ruler dials to post its bundled rule groups to its own `listener`.
///
/// A listener on every interface, `0.0.0.0` or `::`, gets the loopback address
/// of the same family. A listener on one address gets that address.
#[must_use]
pub fn bundled_rules_address(listener: SocketAddr) -> SocketAddr {
    let ip = match listener.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        ip => ip,
    };
    SocketAddr::new(ip, listener.port())
}
