use super::{Arc, CancellationToken, DistributorState, SocketAddr, router};

/// Serve the distributor until cancelled, returning the bound address and the
/// accept loop's handle.
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
    shutdown: CancellationToken,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    let app = krabka_observability::contain_handler_panics(router(state));
    let handle = tokio::spawn(async move {
        let server = axum::serve(listener, app)
            .with_graceful_shutdown(async move { shutdown.cancelled().await });
        if let Err(err) = server.await {
            tracing::error!(error = %err, "traces distributor server stopped");
        }
    });
    Ok((bound, handle))
}
