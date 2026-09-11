use super::{
    Arc, DistributorState, Future, ServerListener, ServerSecurity, SocketAddr, TcpListener, router,
    serve_router,
};

/// Binds and serves the metrics distributor until `shutdown` resolves.
///
/// `security` decides whether the listener serves TLS and whether a request
/// needs a credential. `ServerSecurity::default()` serves plain HTTP with no
/// authentication, as Grafana Mimir does by default.
///
/// # Errors
///
/// Returns an error when the address cannot be bound, or when the bound
/// socket cannot report its local address.
pub async fn serve(
    addr: SocketAddr,
    state: Arc<DistributorState>,
    security: &ServerSecurity,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr> {
    let listener = ServerListener::bind(TcpListener::bind(addr).await?, security)
        .map_err(std::io::Error::other)?;
    let bound = listener.local_addr();
    let server = serve_router(listener, router(state), security)
        .with_graceful_shutdown(shutdown)
        .into_future();
    tokio::spawn(async move {
        if let Err(error) = server.await {
            tracing::warn!(%error, "metrics distributor server stopped with error");
        }
    });
    Ok(bound)
}
