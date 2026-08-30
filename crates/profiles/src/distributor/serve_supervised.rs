use super::{Arc, DistributorState, SocketAddr, TcpListener, router};

/// Serve the distributor and cancel the role when the HTTP server fails.
///
/// # Errors
/// Returns an error when the listener cannot be bound.
pub async fn serve_supervised(
    addr: SocketAddr,
    state: Arc<DistributorState>,
    shutdown: tokio_util::sync::CancellationToken,
) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tokio::spawn(async move {
        let server_shutdown = shutdown.clone();
        if let Err(err) = axum::serve(listener, router(state))
            .with_graceful_shutdown(async move { server_shutdown.cancelled().await })
            .await
        {
            tracing::error!(%err, "profiles distributor server stopped");
            shutdown.cancel();
        }
    });
    Ok(bound)
}
