use super::*;

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn build_querier_state(
    config: &ServiceConfig,
    object_store: Option<&dyn ObjectStore>,
) -> Result<QuerierState, ServiceConfigError> {
    build_querier_state_with_object_store_prefix(config, object_store, None).await
}
