use super::{
    CancellationToken, CriticalTaskError, ObjectStore, Role, ServiceConfig, ServiceDependencies,
    ServiceRuntimeError, SupervisedTasks, TcpListener, build_service_router_with_shutdown,
    contain_handler_panics, serve_compactor_service_listener, shutdown_signal,
};

/// Serves a role on `listener` until it is asked to stop or one of its
/// background tasks stops first.
///
/// The listener and the role's background tasks are joined, not detached. A
/// task that returns, errors, or panics cancels the shutdown token and fails
/// this call with [`ServiceRuntimeError::CriticalTask`], so the process exits
/// instead of holding the port open over a WAL consumer that is no longer
/// consuming.
///
/// A panic inside a request handler is the opposite case and is contained: the
/// router answers 500 for that request and keeps the connection.
///
/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, the configured storage or export backend fails, or a critical background task stops.
pub async fn serve_service_listener(
    listener: TcpListener,
    config: ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<(), ServiceRuntimeError> {
    if config.target == Role::Compactor {
        return serve_compactor_service_listener(listener, config, dependencies, object_store)
            .await;
    }

    let token = CancellationToken::new();
    let token_sig = token.clone();
    // Not supervised: this task is meant to finish, and finishing is how it
    // does its job.
    tokio::spawn(async move {
        shutdown_signal().await;
        token_sig.cancel();
    });
    let token_srv = token.clone();
    let (app, background_tasks) =
        build_service_router_with_shutdown(&config, dependencies, object_store, token.clone())
            .await?;
    let server = axum::serve(listener, contain_handler_panics(app))
        .with_graceful_shutdown(async move { token_srv.cancelled().await })
        .into_future();
    tokio::pin!(server);
    let mut tasks = SupervisedTasks::new(token);
    for (name, handle) in background_tasks {
        tasks.adopt(name, handle);
    }
    let outcome = tokio::select! {
        result = &mut server => result.map_err(ServiceRuntimeError::from),
        name = tasks.first_unexpected_exit() => {
            // `first_unexpected_exit` has cancelled the token, so the server
            // is already draining. Let it finish before reporting.
            if let Err(error) = server.await {
                tracing::warn!(%error, "HTTP server stopped with an error while draining");
            }
            Err(CriticalTaskError(name).into())
        }
    };
    tasks.shutdown().await;
    outcome
}
