use super::{
    Arc, DistributorState, ServerListener, ServerSecurity, SocketAddr, TcpListener, router,
    serve_router,
};

/// Serve the distributor, returning the bound address and the accept loop's
/// handle for the caller to supervise.
///
/// The router carries `/ready` for `readiness`, so a probe can ask the port it
/// pushes to and not only the admin port. The listener serves TLS and
/// authenticates each request as `security` says, and `GET /ready` needs no
/// credential.
///
/// The handle matters because a distributor whose accept loop has stopped is
/// indistinguishable from one nobody is pushing to. Supervising it turns that
/// into an exit. A panic inside a handler is contained instead: that request
/// gets a 500 and the loop carries on.
///
/// # Errors
/// Returns an error when the listener cannot be bound, or when the socket
/// cannot report its local address.
pub async fn serve_supervised(
    addr: SocketAddr,
    state: Arc<DistributorState>,
    readiness: krabka_observability::RoleReadiness,
    security: &ServerSecurity,
    shutdown: tokio_util::sync::CancellationToken,
) -> std::io::Result<(SocketAddr, tokio::task::JoinHandle<()>)> {
    let listener = ServerListener::bind(TcpListener::bind(addr).await?, security)
        .map_err(std::io::Error::other)?;
    let bound = listener.local_addr();
    let app = router(state).merge(krabka_observability::readiness_router(readiness));
    let server = serve_router(listener, app, security)
        .with_graceful_shutdown(async move { shutdown.cancelled().await });
    let handle = tokio::spawn(async move {
        if let Err(err) = server.await {
            tracing::error!(%err, "profiles distributor server stopped");
        }
    });
    Ok((bound, handle))
}
