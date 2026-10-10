use krabka_observability::server_security::{RouterServer, spawn_router_server};

use super::{Arc, DistributorState, Future, ServerSecurity, SocketAddr, router};

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
    spawn_router_server(RouterServer {
        addr,
        router: router(state),
        security,
        shutdown,
        server_name: "profiles distributor",
    })
    .await
}
