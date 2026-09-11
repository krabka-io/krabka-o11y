use super::{
    CancellationToken, CriticalTaskError, ObjectStore, Role, ServerListener, ServiceConfig,
    ServiceDependencies, ServiceRuntimeError, SupervisedTasks, TcpListener,
    build_service_router_with_shutdown, serve_all_service_listener,
    serve_compactor_service_listener, serve_router, shutdown_signal, start_runtime_security,
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
/// The listener serves TLS and authenticates requests as the
/// `server_security` flags of `config` say, and it serves plain HTTP with no
/// authentication when they are unset. The role records its audit events as
/// the `audit` flags say, and it supervises the audit writer with its other
/// tasks.
///
/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, the configured storage or export backend fails, a security flag does not load, the audit layer does not start, or a critical background task stops.
pub async fn serve_service_listener(
    listener: TcpListener,
    config: ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<(), ServiceRuntimeError> {
    if config.target == Role::BlockBuilder {
        return Box::pin(serve_compactor_service_listener(
            listener,
            config,
            dependencies,
            object_store,
        ))
        .await;
    }
    if config.target == Role::All {
        let token = CancellationToken::new();
        let token_sig = token.clone();
        // Not supervised: this task is meant to finish, and finishing is how
        // it does its job.
        tokio::spawn(async move {
            shutdown_signal().await;
            token_sig.cancel();
        });
        // Boxed: this future carries the whole all-in-one start-up, which is
        // several KB, and would otherwise be inlined into every caller of
        // `serve_service_listener` including the single-role ones.
        return Box::pin(serve_all_service_listener(
            listener,
            config,
            dependencies,
            object_store,
            token,
        ))
        .await;
    }

    let security = Box::pin(start_runtime_security(&config, &dependencies)).await?;
    // Cancelled on every way out of this function, so an early error does not
    // leave the audit writer waiting for a stop that never comes.
    let _audit_stop = security.audit_stop.clone().drop_guard();
    let dependencies = dependencies.with_audit(security.audit.clone());
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
    let listener = ServerListener::bind(listener, &security.server)?;
    let server = serve_router(listener, app, &security.server)
        .with_graceful_shutdown(async move { token_srv.cancelled().await })
        .into_future();
    tokio::pin!(server);
    let mut tasks = SupervisedTasks::new(token);
    for (name, handle) in background_tasks {
        tasks.adopt(name, handle);
    }
    if let Some(writer) = security.audit_writer {
        tasks.adopt("audit writer", writer);
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
    // The listener has stopped, so no request records an event after this.
    security.audit_stop.cancel();
    tasks.shutdown().await;
    outcome
}
