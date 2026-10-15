use std::{io, net::SocketAddr, sync::Arc, time::Duration};

use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinSet,
};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;

use super::{ClientIdentity, PeerAddr, ServerStream, listener_mode::HandshakenConnection};

/// Reports a failed `accept`, and waits before the next one when the error is not a single connection's problem.
///
/// This copies `axum::serve`'s own `TcpListener` behaviour. A connection that
/// the client reset before the accept is not a fault. Any other error, such as
/// `EMFILE`, is logged and followed by a one-second pause, so the loop does not
/// spin while the process has no file descriptors left.
pub async fn handle_accept_error(error: io::Error) {
    if matches!(
        error.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
    ) {
        return;
    }
    tracing::error!(%error, "accept error; retrying in one second");
    tokio::time::sleep(Duration::from_secs(1)).await;
}

/// Accepts TCP connections and runs one TLS handshake task for each, until `stop` is cancelled.
///
/// Each finished connection goes to `ready`, which `accept` reads.
///
/// `axum::serve::Listener::accept` cannot fail and is awaited once per
/// connection, one at a time. A handshake inside `accept` would let one client
/// that connects and then sends nothing stop every other client. So this loop
/// only accepts, and each handshake runs concurrently in `in_progress`. When the
/// loop ends it drops `in_progress`, which aborts the handshakes still running,
/// and it drops `listener`, which closes the socket.
pub async fn run_tls_accept_loop(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    handshake_timeout: Duration,
    verifies_client_certificates: bool,
    ready: mpsc::Sender<HandshakenConnection>,
    stop: CancellationToken,
) {
    let mut in_progress = JoinSet::new();
    loop {
        tokio::select! {
            () = stop.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((tcp, socket)) => {
                    in_progress.spawn(tls_handshake(
                        acceptor.clone(),
                        tcp,
                        socket,
                        handshake_timeout,
                        verifies_client_certificates,
                        ready.clone(),
                    ));
                }
                Err(error) => {
                    tokio::select! {
                        () = stop.cancelled() => break,
                        () = handle_accept_error(error) => {}
                    }
                }
            },
            Some(joined) = in_progress.join_next(), if !in_progress.is_empty() => {
                if let Err(error) = joined {
                    tracing::error!(%error, "TLS handshake task panicked");
                }
            }
        }
    }
}

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
    handshaken: mpsc::Sender<HandshakenConnection>,
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
        .send(HandshakenConnection {
            stream: ServerStream::Tls(Box::new(tls)),
            peer,
        })
        .await;
}
