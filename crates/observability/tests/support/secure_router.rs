//! A router served on `127.0.0.1:0` through the server security listener,
//! shared by the `server_security` and `service_security` suites.
//!
//! Both reach this file with `#[path]`, so it names only external crates.

use std::{future::IntoFuture as _, net::SocketAddr};

use axum::Router;
use krabka_observability::{
    CancellationToken,
    server_security::{ServerListener, ServerSecurity, serve_router},
};
use tokio::net::TcpListener;

/// The address a [`serve_secure_router`] call listens on, and the token that
/// shuts it down gracefully.
pub struct SecureRouter {
    pub addr: SocketAddr,
    pub stop: CancellationToken,
}

/// Serves `router` behind `security` on a free local port until
/// [`SecureRouter::stop`] is cancelled.
pub async fn serve_secure_router(router: Router, security: &ServerSecurity) -> SecureRouter {
    let tcp = TcpListener::bind("127.0.0.1:0").await.expect("a free port");
    let listener = ServerListener::bind(tcp, security).expect("the listener binds");
    let addr = listener.local_addr();
    let stop = CancellationToken::new();
    tokio::spawn(
        serve_router(listener, router, security)
            .with_graceful_shutdown(stop.clone().cancelled_owned())
            .into_future(),
    );
    SecureRouter { addr, stop }
}
