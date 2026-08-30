use super::{Router, SocketAddr, serve_prometheus_router_joinable};

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn serve_prometheus_router(
    addr: SocketAddr,
    router: Router,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr> {
    let (bound, _server) = serve_prometheus_router_joinable(addr, router, shutdown).await?;
    Ok(bound)
}
