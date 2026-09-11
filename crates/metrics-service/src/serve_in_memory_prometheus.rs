use krabka_observability::server_security::ServerSecurity;

use super::{SocketAddr, in_memory_prometheus_router, serve_prometheus_router};

/// Serves the Prometheus API over an empty in-memory store at `addr`, until `shutdown` resolves.
///
/// # Errors
/// Returns an error when `addr` cannot be bound, or when the bound socket
/// cannot report its local address.
pub async fn serve_in_memory_prometheus(
    addr: SocketAddr,
    security: &ServerSecurity,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr> {
    serve_prometheus_router(addr, in_memory_prometheus_router(), security, shutdown).await
}
