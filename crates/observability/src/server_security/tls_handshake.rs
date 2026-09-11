use std::{net::SocketAddr, sync::Arc, time::Duration};

use tokio::{net::TcpStream, sync::mpsc};
use tokio_rustls::TlsAcceptor;

use super::{ClientIdentity, PeerAddr, ServerStream};

/// Runs one TLS handshake, and sends the finished connection to the listener.
///
/// A handshake that fails or does not finish inside `timeout` is logged at
/// `debug` and dropped. It never reaches `accept`, so a bad client costs
/// only its own connection.
pub async fn tls_handshake(
    acceptor: TlsAcceptor,
    tcp: TcpStream,
    socket: SocketAddr,
    timeout: Duration,
    verifies_client_certificates: bool,
    handshaken: mpsc::Sender<(ServerStream, PeerAddr)>,
) {
    let tls = match tokio::time::timeout(timeout, acceptor.accept(tcp)).await {
        Ok(Ok(tls)) => tls,
        Ok(Err(error)) => {
            tracing::debug!(peer = %socket, %error, "TLS handshake failed; dropping the connection");
            return;
        }
        Err(_) => {
            tracing::debug!(
                peer = %socket,
                timeout_secs = timeout.as_secs_f64(),
                "TLS handshake did not finish in time; dropping the connection"
            );
            return;
        }
    };
    // rustls keeps a peer certificate only after its client verifier accepted
    // it. Without a verifier the listener never asked for one, so the check on
    // `verifies_client_certificates` keeps an unverified certificate from ever
    // becoming an identity.
    let leaf = if verifies_client_certificates {
        tls.get_ref()
            .1
            .peer_certificates()
            .and_then(|chain| chain.first())
    } else {
        None
    };
    let client_identity = match leaf.map(|leaf| ClientIdentity::from_der(leaf)) {
        None => None,
        Some(Ok(identity)) => Some(Arc::new(identity)),
        Some(Err(error)) => {
            tracing::debug!(
                peer = %socket,
                %error,
                "verified client certificate has no readable identity; dropping the connection"
            );
            return;
        }
    };
    let peer = PeerAddr {
        socket,
        client_identity,
    };
    // The send fails only when the listener is gone, and then nobody is left
    // to serve the connection.
    let _ = handshaken
        .send((ServerStream::Tls(Box::new(tls)), peer))
        .await;
}
