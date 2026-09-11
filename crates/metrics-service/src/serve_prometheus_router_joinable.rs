use krabka_observability::server_security::{ServerListener, ServerSecurity, serve_router};

use super::{JoinHandle, Router, SocketAddr, TcpListener};

/// Like [`serve_prometheus_router`], but returns the spawned server task to the
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
) -> std::io::Result<(SocketAddr, JoinHandle<()>)> {
    let listener = ServerListener::bind(TcpListener::bind(addr).await?, security)
        .map_err(std::io::Error::other)?;
    let bound = listener.local_addr();
    let serving = serve_router(listener, router, security)
        .with_graceful_shutdown(shutdown)
        .into_future();
    let server = tokio::spawn(async move {
        if let Err(error) = serving.await {
            tracing::warn!(%error, "metrics prometheus server stopped with error");
        }
    });
    Ok((bound, server))
}
