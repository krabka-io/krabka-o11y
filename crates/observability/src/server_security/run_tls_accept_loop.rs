use std::time::Duration;

use tokio::{net::TcpListener, sync::mpsc, task::JoinSet};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;

use super::{
    PeerAddr, ServerStream, handle_accept_error::handle_accept_error, tls_handshake::tls_handshake,
};

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
    ready: mpsc::Sender<(ServerStream, PeerAddr)>,
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
