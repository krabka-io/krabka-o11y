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
/// # Errors
/// Returns an error if the operation cannot be completed.
pub async fn serve_prometheus_router_joinable(
    addr: SocketAddr,
    router: Router,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<(SocketAddr, JoinHandle<()>)> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    let server = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router)
            .with_graceful_shutdown(shutdown)
            .await
        {
            tracing::warn!(%error, "metrics prometheus server stopped with error");
        }
    });
    Ok((bound, server))
}
