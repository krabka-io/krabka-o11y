use super::{Arc, DistributorState, Future, SocketAddr, TcpListener, router};

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn serve(
    addr: SocketAddr,
    state: Arc<DistributorState>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, router(state))
            .with_graceful_shutdown(shutdown)
            .await
        {
            tracing::warn!(%err, "profiles distributor server stopped with error");
        }
    });
    Ok(bound)
}
