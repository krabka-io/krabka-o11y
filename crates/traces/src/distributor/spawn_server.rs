use std::{fmt::Display, future::Future};

use super::{ServerListener, ServerSecurity, SocketAddr};

/// Bind a listener on `addr` that serves as `security` says.
pub(crate) async fn bind_listener(
    addr: SocketAddr,
    security: &ServerSecurity,
) -> std::io::Result<ServerListener> {
    let tcp = tokio::net::TcpListener::bind(addr).await?;
    ServerListener::bind(tcp, security).map_err(std::io::Error::other)
}

/// Spawn `server` and log `name` with the error if it stops with one.
pub(crate) fn spawn_server<E: Display>(
    server: impl Future<Output = Result<(), E>> + Send + 'static,
    name: &'static str,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(err) = server.await {
            tracing::error!(error = %err, "{name} stopped");
        }
    })
}
