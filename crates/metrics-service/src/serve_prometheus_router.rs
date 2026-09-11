use krabka_observability::server_security::ServerSecurity;

use super::{Router, SocketAddr, serve_prometheus_router_joinable};

/// Binds `addr` and serves `router` with `security` until `shutdown` resolves.
///
/// This is [`serve_prometheus_router_joinable`] with the server task left
/// detached, so it returns only the bound address. A service binary should
/// join its server task, and so it uses the joinable function.
///
/// # Errors
/// Returns an error when `addr` cannot be bound, or when the bound socket
/// cannot report its local address.
pub async fn serve_prometheus_router(
    addr: SocketAddr,
    router: Router,
    security: &ServerSecurity,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<SocketAddr> {
    let (bound, _server) =
        serve_prometheus_router_joinable(addr, router, security, shutdown).await?;
    Ok(bound)
}
