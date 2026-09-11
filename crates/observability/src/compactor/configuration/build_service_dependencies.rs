use super::{
    ClientResourcePolicy, ServiceConfig, ServiceDependencies, ServiceRuntimeError,
    build_service_dependencies_with_client_resource_policy,
};
use crate::wal_consumer_metrics::WalConsumerMetrics;

/// Builds role dependencies with the default Kafka client policy, under the
/// WAL client security that `config` names.
///
/// `metrics` is the WAL consumer bundle the compactor's consumer records into.
///
/// # Errors
/// Returns an error when the WAL client security flags do not load, or when a
/// required Kafka dependency cannot connect.
pub async fn build_service_dependencies(
    config: &ServiceConfig,
    metrics: WalConsumerMetrics,
) -> Result<ServiceDependencies, ServiceRuntimeError> {
    let security = config.wal_client_security.load()?;
    build_service_dependencies_with_client_resource_policy(
        config,
        ClientResourcePolicy::default(),
        security,
        metrics,
    )
    .await
}
