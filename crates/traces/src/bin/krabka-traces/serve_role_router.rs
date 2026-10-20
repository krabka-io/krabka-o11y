use krabka_observability::{CriticalTaskError, SupervisedTasks};

use super::{CancellationToken, ProcessSecurity, ServerListener, serve_router};

/// One role's HTTP server and the tasks it supervises.
pub(crate) struct RoleServer<'a> {
    /// Names the role in the listening log line.
    pub(crate) role: &'static str,
    pub(crate) listener: tokio::net::TcpListener,
    pub(crate) router: axum::Router,
    pub(crate) security: &'a ProcessSecurity,
    pub(crate) tasks: SupervisedTasks,
    pub(crate) shutdown: CancellationToken,
}

/// Serve `router` on `listener` for the role named `role` until shutdown, or
/// until one of `tasks` exits unexpectedly, and then shut `tasks` down.
pub(crate) async fn serve_role_router(
    server: RoleServer<'_>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let RoleServer {
        role,
        listener,
        router,
        security,
        mut tasks,
        shutdown,
    } = server;
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
