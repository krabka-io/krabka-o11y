use super::{
    CancellationToken, ObjectStore, Role, ServiceConfig, ServiceDependencies, ServiceRuntimeError,
    TcpListener, build_service_router_with_shutdown, serve_compactor_service_listener,
    shutdown_signal,
};

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
/// # Panics
/// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
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
    tokio::spawn(async move {
        shutdown_signal().await;
        token_sig.cancel();
    });
    let token_srv = token.clone();
    let (app, background_tasks) =
        build_service_router_with_shutdown(&config, dependencies, object_store, token.clone())
            .await?;
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async move { token_srv.cancelled().await })
        .into_future();
    tokio::pin!(server);
    let mut tasks = tokio::task::JoinSet::new();
    for (name, handle) in background_tasks {
        tasks.spawn(async move {
            let result = handle.await;
            (name, result)
        });
    }
    if tasks.is_empty() {
        server.await?;
    } else {
        tokio::select! {
            result = &mut server => result?,
            result = tasks.join_next() => {
                if token.is_cancelled() {
                    server.await?;
                } else {
                    let name = result
                        .and_then(Result::ok)
                        .map_or("unknown", |(name, _)| name);
                    token.cancel();
                    return Err(ServiceRuntimeError::CriticalTask(name));
                }
            }
        }
    }
    token.cancel();
    while tasks.join_next().await.is_some() {}
    Ok(())
}
