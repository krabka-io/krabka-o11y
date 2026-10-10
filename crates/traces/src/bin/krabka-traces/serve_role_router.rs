use krabka_observability::{CriticalTaskError, SupervisedTasks};

use super::{CancellationToken, ProcessSecurity, ServerListener, serve_router};

/// Serve `router` on `listener` for the role named `role` until shutdown, or
/// until one of `tasks` exits unexpectedly, and then shut `tasks` down.
pub(crate) async fn serve_role_router(
    role: &'static str,
    listener: tokio::net::TcpListener,
    router: axum::Router,
    security: &ProcessSecurity,
    mut tasks: SupervisedTasks,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = ServerListener::bind(listener, &security.server)?;
    let bound = listener.local_addr();
    tracing::info!(%bound, "{role} listening");
    let server = serve_router(listener, router, &security.server)
        .with_graceful_shutdown(shutdown.cancelled_owned());
    let outcome = tokio::select! {
        result = server => result.map_err(Into::into),
        name = tasks.first_unexpected_exit() => Err(
            Box::<dyn std::error::Error + Send + Sync>::from(CriticalTaskError(name)),
        ),
    };
    tasks.shutdown().await;
    outcome
}
