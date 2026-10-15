use super::{JoinHandle, Router, SocketAddr, in_memory_prometheus_router};
use krabka_observability::server_security::{
    RouterServer, ServerSecurity, spawn_router_server_joinable,
};


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
    let server = serve_prometheus_router_joinable(addr, router, security, shutdown).await?;
    Ok(server.addr)
}

pub struct JoinableRouterServer {
    pub addr: SocketAddr,
    pub server: JoinHandle<()>,
}

/// Like [`crate::serve_prometheus_router`], but returns the spawned server task to the
/// caller.
///
/// Await the returned [`JoinHandle`] after you signal `shutdown`. The process
/// then drains in-flight requests with axum's `with_graceful_shutdown` before
/// it stops, instead of a detached drop of the task.
///
/// The long-running service binaries use this function. They join the handle
/// before they return from their `run_*` entry points.
///
/// `security` decides whether the listener serves TLS and whether a request
/// needs a credential. The router runs inside the authentication layer, so
/// every handler finds the request's principal. `ServerSecurity::default()`
/// serves plain HTTP with no authentication, as Grafana Mimir does by default.
///
/// A panic inside a handler, or inside the authentication layer, answers 500
/// for that request instead of dropping the connection. This is the serving
/// boundary for every `krabka-metrics-service` role, including the Prometheus
/// router that `krabka-promql` builds, so one wrap here covers all of them.
///
/// # Errors
/// Returns an error when `addr` cannot be bound, or when the bound socket
/// cannot report its local address.
pub async fn serve_prometheus_router_joinable(
    addr: SocketAddr,
    router: Router,
    security: &ServerSecurity,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<JoinableRouterServer> {
    let server = spawn_router_server_joinable(RouterServer {
        addr,
        router,
        security,
        shutdown,
        server_name: "metrics prometheus",
    })
    .await?;
    Ok(JoinableRouterServer {
        addr: server.address,
        server: server.task,
    })
}
