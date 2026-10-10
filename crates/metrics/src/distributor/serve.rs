use krabka_observability::server_security::{RouterServer, spawn_router_server};

use super::{Arc, DistributorState, Future, ServerSecurity, SocketAddr, router};

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
    spawn_router_server(RouterServer {
        addr,
        router: router(state),
        security,
        shutdown,
        server_name: "metrics distributor",
    })
    .await
}
