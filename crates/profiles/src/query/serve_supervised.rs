use super::{Arc, ProfileStore, QuerierState, SocketAddr, TcpListener, router};

/// Serve the querier, returning the bound address and the accept loop's handle
/// for the caller to supervise.
///
/// The router carries `/ready` for `readiness`, so a query-frontend or an
/// orchestrator can ask the port it queries whether this backend can answer
/// yet, rather than learning from empty results that it could not.
///
/// The handle matters because a querier whose accept loop has stopped still
/// holds its port open to a liveness probe. Supervising it turns that into an
/// exit. A panic inside a handler is contained instead: that request gets a
/// 500 and the loop carries on.
///
/// # Errors
/// Returns an error when the listener cannot be bound.
pub async fn serve_supervised<S>(
    addr: SocketAddr,
    state: Arc<QuerierState<S>>,
    readiness: krabka_observability::RoleReadiness,
    shutdown: tokio_util::sync::CancellationToken,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<()>)>
where
    S: ProfileStore + 'static,
{
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    let app = krabka_observability::contain_handler_panics(
        router(state).merge(krabka_observability::readiness_router(readiness)),
    );
    let handle = tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app)
            .with_graceful_shutdown(async move { shutdown.cancelled().await })
            .await
        {
            tracing::error!(%err, "profiles querier server stopped");
        }
    });
    Ok((bound, handle))
}
