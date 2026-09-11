use super::{
    ObjectStore, ServiceConfig, ServiceDependencies, ServiceRuntimeError, TcpListener,
    serve_service_listener,
};

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn serve_service(
    config: ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<(), ServiceRuntimeError> {
    let listener = TcpListener::bind(config.listen_addr).await?;
    // Boxed: the role start-ups this dispatches to are several KB of future
    // between them, and inlining them here would put all of that on the stack
    // of whatever awaits `serve_service`.
    Box::pin(serve_service_listener(
        listener,
        config,
        dependencies,
        object_store,
    ))
    .await
}
