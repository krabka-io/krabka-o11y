use super::*;

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn serve_service(
    config: ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<(), ServiceRuntimeError> {
    let listener = TcpListener::bind(config.listen_addr).await?;
    serve_service_listener(listener, config, dependencies, object_store).await
}
