use tokio::{net::TcpListener, sync::mpsc};
use tokio_util::sync::DropGuard;

use super::{PeerAddr, ServerStream};

/// How a [`ServerListener`](super::ServerListener) gets its next connection.
#[derive(Debug)]
pub enum ListenerMode {
    /// Accept plain TCP connections directly.
    Plain(TcpListener),
    /// Read connections whose TLS handshake has finished.
    ///
    /// A background task owns the socket and runs the handshakes. Dropping
    /// `_stop` cancels that task, which closes the socket and aborts every
    /// handshake still in progress.
    Tls {
        handshaken: mpsc::Receiver<(ServerStream, PeerAddr)>,
        _stop: DropGuard,
    },
}
