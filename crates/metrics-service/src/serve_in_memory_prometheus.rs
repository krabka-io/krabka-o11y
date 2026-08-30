use super::{SocketAddr, in_memory_prometheus_router, serve_prometheus_router};

///
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn serve_in_memory_prometheus(
    addr: SocketAddr,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr> {
    serve_prometheus_router(addr, in_memory_prometheus_router(), shutdown).await
}
