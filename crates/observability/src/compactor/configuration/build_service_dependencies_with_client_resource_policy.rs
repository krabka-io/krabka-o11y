use super::{
    ClientResourcePolicy, ClientSecurity, Role, ServiceConfig, ServiceDependencies,
    ServiceRuntimeError, all_in_one_querier_group_id, with_block_builder_dependencies,
    with_distributor_dependencies, with_querier_dependencies,
};
use crate::wal_consumer_metrics::WalConsumerMetrics;

/// Builds role dependencies with one validated Kafka client policy.
///
/// `security` is the WAL client security that
/// `WalClientSecurityArgs::load` gave. Every broker connection of the role
/// uses it, and the dependencies keep it for the audit producer. `None`
/// connects in plain text.
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
    security: Option<ClientSecurity>,
    metrics: WalConsumerMetrics,
) -> Result<ServiceDependencies, ServiceRuntimeError> {
    let dependencies = ServiceDependencies {
        wal_security: security.clone(),
        ..ServiceDependencies::default()
    };
    let security = security.as_ref();
    match config.target {
        Role::Distributor => {
            with_distributor_dependencies(dependencies, config, client_resource_policy, security)
                .await
        }
        Role::BlockBuilder => {
            with_block_builder_dependencies(
                dependencies,
                config,
                config.wal_group_id.clone(),
                client_resource_policy,
                security,
                metrics,
            )
            .await
        }
        Role::Querier => with_querier_dependencies(
            dependencies,
            config,
            config.wal_group_id.clone(),
            client_resource_policy,
            security,
            metrics,
        ),
        Role::All => {
            let dependencies = with_distributor_dependencies(
                dependencies,
                config,
                client_resource_policy,
                security,
            )
            .await?;
            let dependencies = with_block_builder_dependencies(
                dependencies,
                config,
                config.wal_group_id.clone(),
                client_resource_policy,
                security,
                metrics.clone(),
            )
            .await?;
            with_querier_dependencies(
                dependencies,
                config,
                all_in_one_querier_group_id(&config.wal_group_id),
                client_resource_policy,
                security,
                metrics,
            )
        }
    }
}
