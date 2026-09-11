use super::{
    BrokerBackedIngestLimiter, ClientResourcePolicy, KafkaLogWalSink, ServiceConfig,
    ServiceConfigError, ServiceDependencies, ServiceRuntimeError, connect_with_startup_retry,
    validate_distributor_policy,
};

/// Adds what the distributor role cannot take a push without: the WAL sink it
/// writes to, and the broker-backed limiter it checks a tenant's quota
/// against.
pub(crate) async fn with_distributor_dependencies(
    dependencies: ServiceDependencies,
    config: &ServiceConfig,
    client_resource_policy: ClientResourcePolicy,
) -> Result<ServiceDependencies, ServiceRuntimeError> {
    validate_distributor_policy(config)?;
    let bootstrap = config
        .wal_bootstrap_server
        .as_deref()
        .ok_or(ServiceConfigError::MissingWalBootstrapServer)?;
    let bootstrap_owned = bootstrap.to_string();
    let topic = config.wal_topic.clone();
    let sink = connect_with_startup_retry(
        "wal-sink",
        config.wal_connect_startup_deadline,
        config.wal_connect_attempt_timeout,
        config.wal_connect_initial_backoff,
        config.wal_connect_max_backoff,
        || {
            let b = bootstrap_owned.clone();
            let t = topic.clone();
            async move {
                KafkaLogWalSink::connect_with_client_resource_policy(&b, t, client_resource_policy)
                    .await
            }
        },
    )
    .await?;
    let bootstrap_owned2 = bootstrap.to_string();
    let topic2 = config.wal_topic.clone();
    let limiter = connect_with_startup_retry(
        "ingest-limiter",
        config.wal_connect_startup_deadline,
        config.wal_connect_attempt_timeout,
        config.wal_connect_initial_backoff,
        config.wal_connect_max_backoff,
        || {
            let b = bootstrap_owned2.clone();
            let t = topic2.clone();
            async move {
                BrokerBackedIngestLimiter::connect(
                    &b,
                    t,
                    client_resource_policy,
                    config.ingest_quota_burst_window,
                )
                .await
            }
        },
    )
    .await?;
    Ok(dependencies
        .with_wal_sink(sink)
        .with_ingest_limiter(limiter))
}
