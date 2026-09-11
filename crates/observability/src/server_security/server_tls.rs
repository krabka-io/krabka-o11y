use std::sync::Arc;

use krabka_units::Time;
use rustls::ServerConfig;

/// The TLS half of a loaded [`ServerSecurity`](super::ServerSecurity).
#[derive(Debug, Clone)]
pub struct ServerTls {
    /// The rustls configuration, built on the `ring` provider.
    pub config: Arc<ServerConfig>,
    /// Whether the configuration verifies client certificates against a
    /// client CA. Only then is a peer certificate an identity.
    pub verifies_client_certificates: bool,
    /// How long one client may take to finish its handshake.
    pub handshake_timeout: Time,
}
