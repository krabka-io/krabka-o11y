use tokio::net::TcpListener;

use super::{AllStage, SocketAddr};

/// Binds `addr` now and returns the address it got, together with the stage
/// that serves `app` there until its token is cancelled.
///
/// The bind happens here rather than inside the stage so that a port already
/// in use fails the start, while the process still has an error to return,
/// instead of being reported later as a role that exited on its own. The bound
/// address comes back from the listener rather than from `addr`, because the
/// caller may have asked for port 0 and would otherwise have nothing to log
/// and no way to reach the role.
///
/// # Errors
/// Returns an error when `addr` cannot be bound.
pub(crate) async fn bind_all_stage_server(
    addr: SocketAddr,
    app: axum::Router,
    role: &'static str,
) -> std::io::Result<(SocketAddr, AllStage)> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    let app = krabka_observability::contain_handler_panics(app);
    let stage: AllStage = Box::new(move |token| {
        Box::pin(async move {
            if let Err(error) = axum::serve(listener, app)
                .with_graceful_shutdown(async move { token.cancelled().await })
                .await
            {
                tracing::error!(%error, role, "profiles all-in-one server stopped");
            }
        })
    });
    Ok((bound, stage))
}
