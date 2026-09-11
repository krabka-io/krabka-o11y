use std::{net::SocketAddr, sync::Arc};

use axum::{extract::connect_info::Connected, serve::IncomingStream};

use super::{ClientIdentity, ServerListener};

/// The remote end of a connection that a [`ServerListener`] accepted.
///
/// A router served with `into_make_service_with_connect_info::<PeerAddr>()`
/// gives every request a `ConnectInfo<PeerAddr>`. [`serve_router`](super::serve_router)
/// does that.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerAddr {
    /// The client's socket address.
    pub socket: SocketAddr,
    /// The client certificate that rustls verified against the client CA.
    ///
    /// This is `None` on a plain TCP connection, on a TLS listener that does
    /// not ask for client certificates, and when the client sent no certificate.
    pub client_identity: Option<Arc<ClientIdentity>>,
}

impl Connected<IncomingStream<'_, ServerListener>> for PeerAddr {
    fn connect_info(stream: IncomingStream<'_, ServerListener>) -> Self {
        stream.remote_addr().clone()
    }
}
