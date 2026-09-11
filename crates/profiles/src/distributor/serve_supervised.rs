use super::{Arc, DistributorState, SocketAddr, TcpListener, router};

/// Serve the distributor, returning the bound address and the accept loop's
/// handle for the caller to supervise.
///
/// The router carries `/ready` for `readiness`, so a probe can ask the port it
/// pushes to and not only the admin port.
///
/// The handle matters because a distributor whose accept loop has stopped is
/// indistinguishable from one nobody is pushing to. Supervising it turns that
/// into an exit. A panic inside a handler is contained instead: that request
/// gets a 500 and the loop carries on.
///
/// # Errors
/// Returns an error when the listener cannot be bound.
pub async fn serve_supervised(
    addr: SocketAddr,
    state: Arc<DistributorState>,
    readiness: krabka_observability::RoleReadiness,
    shutdown: tokio_util::sync::CancellationToken,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
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
            tracing::error!(%err, "profiles distributor server stopped");
        }
    });
    Ok((bound, handle))
}
