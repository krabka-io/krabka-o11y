use std::{io, net::SocketAddr};

use axum::serve::Listener;
use krabka_units::convert::TimeExt;
use tokio::{net::TcpListener, sync::mpsc};
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;

use super::{
    PeerAddr, ServerSecurity, ServerSecurityError, ServerStream,
    handle_accept_error::handle_accept_error, listener_mode::ListenerMode,
    run_tls_accept_loop::run_tls_accept_loop, warn_about_posture::warn_about_posture,
};

/// The most finished TLS handshakes that wait for `accept` at one time.
///
/// When the queue is full, a finished handshake waits for space before it is
/// served. The handshake timeout does not include that wait.
const HANDSHAKEN_QUEUE: usize = 128;

/// An `axum::serve::Listener` that serves plain TCP or TLS, as the [`ServerSecurity`] says.
///
/// Every existing call site keeps `axum::serve(listener, app)`. With TLS
/// configured, a background task accepts connections and runs each handshake
/// concurrently, so a client that never finishes its handshake stops only its
/// own connection. Dropping the listener stops that task and closes the socket.
#[derive(Debug)]
pub struct ServerListener {
    local_addr: SocketAddr,
    mode: ListenerMode,
}

impl ServerListener {
    /// Wraps a bound `listener` with the TLS posture of `security`.
    ///
    /// When `security` leaves TLS or authentication off, this logs one
    /// warning that names what is off.
    ///
    /// # Errors
    ///
    /// Returns [`ServerSecurityError::LocalAddr`] when the socket cannot
    /// report its local address.
    ///
    /// # Panics
    ///
    /// Panics outside a Tokio runtime when `security` configures TLS, because
    /// the accept task cannot start.
    pub fn bind(
        listener: TcpListener,
        security: &ServerSecurity,
    ) -> Result<Self, ServerSecurityError> {
        let local_addr = listener
            .local_addr()
            .map_err(ServerSecurityError::LocalAddr)?;
        warn_about_posture(local_addr, security);
        let Some(tls) = security.tls() else {
            return Ok(Self {
                local_addr,
                mode: ListenerMode::Plain(listener),
            });
        };
        let (sender, handshaken) = mpsc::channel(HANDSHAKEN_QUEUE);
        let stop = CancellationToken::new();
        tokio::spawn(run_tls_accept_loop(
            listener,
            TlsAcceptor::from(tls.config.clone()),
            tls.handshake_timeout.to_std(),
            tls.verifies_client_certificates,
            sender,
            stop.clone(),
        ));
        Ok(Self {
            local_addr,
            mode: ListenerMode::Tls {
                handshaken,
                _stop: stop.drop_guard(),
            },
        })
    }

    /// The local address the listener is bound to.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

impl Listener for ServerListener {
    type Io = ServerStream;
    type Addr = PeerAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match &mut self.mode {
            ListenerMode::Plain(listener) => loop {
                match TcpListener::accept(listener).await {
                    Ok((tcp, socket)) => {
                        let peer = PeerAddr {
                            socket,
                            client_identity: None,
                        };
                        return (ServerStream::Plain(tcp), peer);
                    }
                    Err(error) => handle_accept_error(error).await,
                }
            },
            ListenerMode::Tls { handshaken, .. } => {
                if let Some(connection) = handshaken.recv().await {
                    return connection;
                }
                // The accept task owns the only sender, and it ends only when
                // this listener drops its guard. A closed channel here means
                // the task died, and `accept` has no way to report that.
                tracing::error!(
                    "TLS accept task stopped; the listener accepts no more connections"
                );
                std::future::pending().await
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(PeerAddr {
            socket: self.local_addr,
            client_identity: None,
        })
    }
}
