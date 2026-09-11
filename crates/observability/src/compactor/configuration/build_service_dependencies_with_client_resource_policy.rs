use super::{
    ClientResourcePolicy, Role, ServiceConfig, ServiceDependencies, ServiceRuntimeError,
    all_in_one_querier_group_id, with_block_builder_dependencies, with_distributor_dependencies,
    with_querier_dependencies,
};
use crate::wal_consumer_metrics::WalConsumerMetrics;

/// Builds role dependencies with one validated Kafka client policy.
///
/// `metrics` is the WAL consumer bundle the block builder's consumer records
/// into. Pass the one the service's registry holds. An unregistered bundle
/// leaves the block builder reading its WAL with every consumer series absent.
///
/// [`Role::All`] takes the union: one process runs all three roles, so it
/// needs everything all three need. The two WAL consumers it ends up with are
/// deliberately in different groups -- see
/// [`all_in_one_querier_group_id`](super::all_in_one_querier_group_id).
///
/// # Errors
/// Returns an error when a required Kafka dependency cannot connect.
pub async fn build_service_dependencies_with_client_resource_policy(
    config: &ServiceConfig,
    client_resource_policy: ClientResourcePolicy,
    metrics: WalConsumerMetrics,
) -> Result<ServiceDependencies, ServiceRuntimeError> {
    let dependencies = ServiceDependencies::default();
    match config.target {
        Role::Distributor => {
            with_distributor_dependencies(dependencies, config, client_resource_policy).await
        }
        Role::BlockBuilder => {
            with_block_builder_dependencies(
                dependencies,
                config,
                config.wal_group_id.clone(),
                client_resource_policy,
                metrics,
            )
            .await
        }
        Role::Querier => with_querier_dependencies(
            dependencies,
            config,
            config.wal_group_id.clone(),
            client_resource_policy,
            metrics,
        ),
        Role::All => {
            let dependencies =
                with_distributor_dependencies(dependencies, config, client_resource_policy).await?;
            let dependencies = with_block_builder_dependencies(
                dependencies,
                config,
                config.wal_group_id.clone(),
                client_resource_policy,
                metrics.clone(),
            )
            .await?;
            with_querier_dependencies(
                dependencies,
                config,
                all_in_one_querier_group_id(&config.wal_group_id),
                client_resource_policy,
                metrics,
            )
        }
    }
}
