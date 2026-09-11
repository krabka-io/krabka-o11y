use std::future::IntoFuture as _;

use super::{
    Arc, CancellationToken, DistributorState, ServerListener, ServerSecurity, SocketAddr, router,
    serve_router,
};

/// Serve the distributor until cancelled, returning the bound address and the
/// accept loop's handle.
///
/// The listener serves as `security` says: plain HTTP with every request
/// unauthenticated when no security flag is set, and TLS, a credential, or
/// both, when the flags ask for them. Each push door then checks that the
/// request's principal may use the tenant it names.
///
/// The handle is half the return value because the caller has to keep it. A
/// listener that stops accepting -- because the loop errored, or because it
/// panicked and unwound past the `if let Err` below -- leaves a distributor
/// process that is still alive, still passing a liveness probe, and no longer
/// taking spans. Supervising the handle is what turns that into an exit.
///
/// A panic inside a handler is the other case and is contained: the router
/// answers 500 for that request and the accept loop carries on.
///
/// # Errors
/// Returns an error when the listener cannot be bound.
pub async fn serve(
    addr: SocketAddr,
    state: Arc<DistributorState>,
    security: &ServerSecurity,
    shutdown: CancellationToken,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let tcp = tokio::net::TcpListener::bind(addr).await?;
    let listener = ServerListener::bind(tcp, security).map_err(std::io::Error::other)?;
    let bound = listener.local_addr();
    let server = serve_router(listener, router(state), security)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .into_future();
    let handle = tokio::spawn(async move {
        if let Err(err) = server.await {
            tracing::error!(error = %err, "traces distributor server stopped");
        }
    });
    Ok((bound, handle))
}
