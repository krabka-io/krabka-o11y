use super::{
    ClientResourcePolicy, ServiceConfig, ServiceDependencies, ServiceRuntimeError,
    build_service_dependencies_with_client_resource_policy,
};
use crate::wal_consumer_metrics::WalConsumerMetrics;

/// Builds role dependencies with the default Kafka client policy.
///
/// `metrics` is the WAL consumer bundle the compactor's consumer records into.
///
/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn build_service_dependencies(
    config: &ServiceConfig,
    metrics: WalConsumerMetrics,
) -> Result<ServiceDependencies, ServiceRuntimeError> {
    build_service_dependencies_with_client_resource_policy(
        config,
        ClientResourcePolicy::default(),
        metrics,
    )
    .await
}
