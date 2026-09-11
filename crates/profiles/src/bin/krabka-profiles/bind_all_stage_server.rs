use tokio::net::TcpListener;

use super::{AllStage, ServerListener, ServerSecurity, SocketAddr, serve_router};

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
/// The listener serves TLS and authenticates each request as `security` says.
///
/// # Errors
/// Returns an error when `addr` cannot be bound, or when the socket cannot
/// report its local address.
pub(crate) async fn bind_all_stage_server(
    addr: SocketAddr,
    app: axum::Router,
    role: &'static str,
    security: &ServerSecurity,
) -> Result<(SocketAddr, AllStage), Box<dyn std::error::Error>> {
    let listener = ServerListener::bind(TcpListener::bind(addr).await?, security)?;
    let bound = listener.local_addr();
    let server = serve_router(listener, app, security);
    let stage: AllStage = Box::new(move |token| {
        Box::pin(async move {
            if let Err(error) = server
                .with_graceful_shutdown(async move { token.cancelled().await })
                .await
            {
                tracing::error!(%error, role, "profiles all-in-one server stopped");
            }
        })
    });
    Ok((bound, stage))
}
