use super::{
    BrokerBackedIngestLimiter, ClientResourcePolicy, KafkaLogWalConsumer, KafkaLogWalSink, Role,
    ServiceConfig, ServiceConfigError, ServiceDependencies, ServiceRuntimeError,
    connect_with_startup_retry, validate_distributor_policy,
};

/// Builds role dependencies with one validated Kafka client policy.
///
/// # Errors
/// Returns an error when a required Kafka dependency cannot connect.
pub async fn build_service_dependencies_with_client_resource_policy(
    config: &ServiceConfig,
    client_resource_policy: ClientResourcePolicy,
) -> Result<ServiceDependencies, ServiceRuntimeError> {
    match config.target {
        Role::Distributor => {
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
                        KafkaLogWalSink::connect_with_client_resource_policy(
                            &b,
                            t,
                            client_resource_policy,
                        )
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
            Ok(ServiceDependencies::default()
                .with_wal_sink(sink)
                .with_ingest_limiter(limiter))
        }
        Role::Compactor => {
            let bootstrap = config
                .wal_bootstrap_server
                .as_deref()
                .ok_or(ServiceConfigError::MissingWalBootstrapServer)?;
            let group_id = config.wal_group_id.clone();
            let topic = config.wal_topic.clone();
            // `Consumer::start` (called by `KafkaLogWalConsumer::connect`) now
            // retries internally with per-attempt timeouts, so there is no need
            // to double-wrap it in `connect_with_startup_retry`.  Call directly.
            let consumer = KafkaLogWalConsumer::connect_with_client_resource_policy(
                bootstrap,
                group_id,
                topic,
                client_resource_policy,
            )
            .await?;
            Ok(ServiceDependencies::default().with_wal_consumer(consumer))
        }
        Role::Querier => {
            // Validate configuration eagerly so misconfiguration fails fast (the
            // `querier_dependencies_require_wal_bootstrap_server` test relies on this).
            let bootstrap = config
                .wal_bootstrap_server
                .as_deref()
                .ok_or(ServiceConfigError::MissingWalBootstrapServer)?;
            // The actual Kafka connects happen asynchronously inside build_service_router so
            // that the querier's HTTP port binds immediately (FIX B2). Store the params for
            // later use.
            Ok(
                ServiceDependencies::default().with_deferred_wal_consumer_connect(
                    bootstrap.to_string(),
                    config.wal_group_id.clone(),
                    config.wal_topic.clone(),
                    client_resource_policy,
                ),
            )
        }
    }
}
