use super::{
    BrokerAccessPolicy, ClientResourcePolicy, ClientSecurity, DeferredQueryAuthorizerConnect,
    DeferredWalConsumerConnect, ServiceConfig, ServiceConfigError, ServiceDependencies,
    ServiceRuntimeError, WalConsumerMetrics,
};

/// Records what the querier's hot tail will need, without connecting it.
///
/// The connect happens in the background once the HTTP port is bound, so the
/// querier answers `/ready` with an unmet `wal-tail` gate rather than
/// answering nothing at all while the broker is still coming up. Only the
/// configuration is validated here, eagerly, so a querier pointed at no broker
/// fails at start-up rather than on its first query.
///
/// `group_id` is taken rather than read from `config` for the reason
/// [`with_block_builder_dependencies`](super::with_block_builder_dependencies)
/// gives: in one process these two roles must not share a consumer group.
///
/// Both deferred connects keep `security`, the WAL client security that the
/// service loaded. `None` connects in plain text.
pub(crate) fn with_querier_dependencies(
    dependencies: ServiceDependencies,
    config: &ServiceConfig,
    group_id: String,
    client_resource_policy: ClientResourcePolicy,
    security: Option<&ClientSecurity>,
    metrics: WalConsumerMetrics,
) -> Result<ServiceDependencies, ServiceRuntimeError> {
    let bootstrap = config
        .wal_bootstrap_server
        .as_deref()
        .ok_or(ServiceConfigError::MissingWalBootstrapServer)?;
    Ok(dependencies
        .with_deferred_query_authorizer_connect(DeferredQueryAuthorizerConnect {
            bootstrap: bootstrap.to_string(),
            topic: config.wal_topic.clone(),
            client_resource_policy,
            security: security.cloned(),
            access_policy: BrokerAccessPolicy::for_config(config)?,
        })
        .with_deferred_wal_consumer_connect(DeferredWalConsumerConnect {
            bootstrap: bootstrap.to_string(),
            group_id,
            topic: config.wal_topic.clone(),
            client_resource_policy,
            security: security.cloned(),
            metrics,
        }))
}
