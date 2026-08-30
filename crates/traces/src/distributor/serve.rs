use super::{Arc, CancellationToken, DistributorState, SocketAddr, router};

/// Serve the distributor until cancelled.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub async fn serve(
    addr: SocketAddr,
    state: Arc<DistributorState>,
    shutdown: CancellationToken,
) -> std::io::Result<SocketAddr> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    let app = router(state);
    tokio::spawn(async move {
        let server_shutdown = shutdown.clone();
        let server = axum::serve(listener, app).with_graceful_shutdown(async move {
            server_shutdown.cancelled().await;
        });
        if let Err(err) = server.await {
            tracing::error!(error = %err, "traces distributor server stopped");
            shutdown.cancel();
        }
    });
    Ok(bound)
}
