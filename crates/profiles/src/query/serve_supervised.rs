use super::{Arc, ProfileStore, QuerierState, SocketAddr, TcpListener, router};

/// Serve the querier and cancel the role when the HTTP server fails.
///
/// The router carries `/ready` for `readiness`, so a query-frontend or an
/// orchestrator can ask the port it queries whether this backend can answer
/// yet, rather than learning from empty results that it could not.
///
/// # Errors
/// Returns an error when the listener cannot be bound.
pub async fn serve_supervised<S>(
    addr: SocketAddr,
    state: Arc<QuerierState<S>>,
    readiness: krabka_observability::RoleReadiness,
    shutdown: tokio_util::sync::CancellationToken,
) -> std::io::Result<SocketAddr>
where
    S: ProfileStore + 'static,
{
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    let app = router(state).merge(krabka_observability::readiness_router(readiness));
    tokio::spawn(async move {
        let server_shutdown = shutdown.clone();
        if let Err(err) = axum::serve(listener, app)
            .with_graceful_shutdown(async move { server_shutdown.cancelled().await })
            .await
        {
            tracing::error!(%err, "profiles querier server stopped");
            shutdown.cancel();
        }
    });
    Ok(bound)
}
