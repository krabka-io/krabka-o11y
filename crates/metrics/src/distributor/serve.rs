use super::{SocketAddr, Arc, DistributorState, Future, TcpListener, router};

/// Binds and serves the metrics distributor until `shutdown` resolves.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn serve(
    addr: SocketAddr,
    state: Arc<DistributorState>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router(state))
            .with_graceful_shutdown(shutdown)
            .await
        {
            tracing::warn!(%error, "metrics distributor server stopped with error");
        }
    });
    Ok(bound)
}
