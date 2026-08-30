use super::*;

/// Serve the querier and cancel the role when the HTTP server fails.
///
/// # Errors
/// Returns an error when the listener cannot be bound.
pub async fn serve_supervised<S>(
    addr: SocketAddr,
    state: Arc<QuerierState<S>>,
    shutdown: tokio_util::sync::CancellationToken,
) -> std::io::Result<SocketAddr>
where
    S: ProfileStore + 'static,
{
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    tokio::spawn(async move {
        let server_shutdown = shutdown.clone();
        if let Err(err) = axum::serve(listener, router(state))
            .with_graceful_shutdown(async move { server_shutdown.cancelled().await })
            .await
        {
            tracing::error!(%err, "profiles querier server stopped");
            shutdown.cancel();
        }
    });
    Ok(bound)
}
