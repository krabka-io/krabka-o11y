use super::*;

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn build_service_dependencies(
    config: &ServiceConfig,
) -> Result<ServiceDependencies, ServiceRuntimeError> {
    build_service_dependencies_with_client_resource_policy(config, ClientResourcePolicy::default())
        .await
}
