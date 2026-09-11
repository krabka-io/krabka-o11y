use super::{
    Arc, DistributorState, Future, ServerListener, ServerSecurity, SocketAddr, TcpListener, router,
    serve_router,
};

/// Serves the distributor on `addr` until `shutdown` completes, and returns the bound address.
///
/// The listener serves TLS and authenticates each request as `security` says.
/// `ServerSecurity::default()` serves plain HTTP and accepts every request, as
/// Pyroscope does.
///
/// # Errors
/// Returns an error when `addr` cannot be bound, or when the socket cannot
/// report its local address.
pub async fn serve(
    addr: SocketAddr,
    state: Arc<DistributorState>,
    security: &ServerSecurity,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr> {
    let listener = ServerListener::bind(TcpListener::bind(addr).await?, security)
        .map_err(std::io::Error::other)?;
    let bound = listener.local_addr();
    let server = serve_router(listener, router(state), security).with_graceful_shutdown(shutdown);
    tokio::spawn(async move {
        if let Err(err) = server.await {
            tracing::warn!(%err, "profiles distributor server stopped with error");
        }
    });
    Ok(bound)
}
