use super::{
    ClientResourcePolicy, ClientSecurity, KafkaLogWalConsumer, ServiceConfig, ServiceDependencies,
    ServiceRuntimeError, WalDependenciesRequest, deferred_query_authorizer_connect,
};

/// Adds the WAL consumer the block builder reads its records from, and the
/// query authorizer its delete-request API checks every tenant against.
///
/// `group_id` is taken rather than read from `config` because an all-in-one
/// process runs this role alongside a querier that also consumes the same
/// topic. Two consumers in one group are given disjoint partitions, so a
/// shared group id would leave the block builder writing blocks from half the
/// WAL and the querier tailing the other half, with nothing to say so.
///
/// Both connect under `security`, the WAL client security that the service
/// loaded. `None` connects in plain text.
pub(crate) async fn with_block_builder_dependencies(
    request: WalDependenciesRequest<'_>,
) -> Result<ServiceDependencies, ServiceRuntimeError> {
    let WalDependenciesRequest {
        dependencies,
        config,
        group_id,
        client_resource_policy,
        security,
        metrics,
    } = request;
    let deferred_authorizer =
        deferred_query_authorizer_connect(config, client_resource_policy, security)?;
    // `Consumer::start` (called by `KafkaLogWalConsumer::connect`) retries
    // internally with per-attempt timeouts, so there is no need to wrap it in
    // `connect_with_startup_retry` as well.
    let consumer = KafkaLogWalConsumer::connect_with_client_resource_policy(
        &deferred_authorizer.bootstrap,
        group_id,
        config.wal_topic.clone(),
        client_resource_policy,
        security.cloned(),
    )
    .await?
    .with_metrics(metrics.clone())
    .rewinding_fenced_partitions();
    Ok(dependencies
        .with_wal_consumer(consumer)
        .with_wal_recovery_metrics(metrics)
        .with_deferred_query_authorizer_connect(deferred_authorizer.connect))
}
