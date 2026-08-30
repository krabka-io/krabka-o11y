use super::{
    CancellationToken, ObjectStore, Router, ServiceConfig, ServiceConfigError, ServiceDependencies,
    build_service_router_with_shutdown,
};

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn build_service_router(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<Router, ServiceConfigError> {
    build_service_router_with_shutdown(config, dependencies, object_store, CancellationToken::new())
        .await
        .map(|(router, _)| router)
}
