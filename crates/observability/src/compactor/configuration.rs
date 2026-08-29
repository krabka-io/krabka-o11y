#[allow(clippy::wildcard_imports)]
use super::*;

pub(crate) fn build_compactor_configured_object_store(
    config: &ServiceConfig,
    object_store: Option<&dyn ObjectStore>,
) -> Result<Option<ConfiguredObjectStore>, ServiceConfigError> {
    if object_store.is_some() {
        return Ok(None);
    }

    build_configured_object_store(config)
}

pub(crate) fn compactor_object_store<'a>(
    object_store: Option<&'a dyn ObjectStore>,
    configured_store: Option<&'a ConfiguredObjectStore>,
) -> Result<(&'a dyn ObjectStore, Option<&'a ObjectPath>), ServiceConfigError> {
    if let Some(store) = object_store {
        return Ok((store, None));
    }

    let configured_store = configured_store.ok_or(ServiceConfigError::MissingObjectStore)?;
    Ok((
        configured_store.store.as_ref(),
        Some(&configured_store.prefix),
    ))
}

#[cfg_attr(test, mutants::skip)]
pub(crate) async fn connect_with_startup_retry<T, E, F, Fut>(
    what: &str,
    deadline: Time,
    attempt_timeout: Time,
    initial_backoff: Time,
    max_backoff: Time,
    mut make: F,
) -> Result<T, E>
where
    E: std::fmt::Display,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    // Every attempt is bounded by `attempt_timeout` — including the
    // final one after the deadline.  The previous implementation called an
    // un-timed `make().await` when the deadline expired inside the timeout arm,
    // which could itself hang forever.  Instead we track the last `Err` value
    // and return it on deadline expiry so we never call make() without a timer.
    let start = tokio::time::Instant::now();
    let mut backoff = initial_backoff;
    let mut last_err: Option<E> = None;
    loop {
        match tokio::time::timeout(attempt_timeout.to_std(), make()).await {
            Ok(Ok(v)) => return Ok(v),
            Ok(Err(error)) => {
                if start.elapsed().as_time() >= deadline {
                    return Err(error);
                }
                tracing::warn!(dependency = what, %error, "WAL dependency connect failed during broker warmup; retrying");
                last_err = Some(error);
            }
            Err(_elapsed) => {
                if start.elapsed().as_time() >= deadline {
                    if let Some(e) = last_err {
                        // Return the last real error we saw rather than an un-timed attempt.
                        return Err(e);
                    }
                    // All attempts so far only timed out (no Err variant captured).
                    // Do one final timed attempt; whatever it returns is the answer.
                    return if let Ok(result) =
                        tokio::time::timeout(attempt_timeout.to_std(), make()).await
                    {
                        result
                    } else {
                        // Still timing out after the deadline — treat as a persistent
                        // hang; update last_err on next loop iteration and eventually
                        // we'll return Err from the Ok(Err) deadline arm.  For now,
                        // sleep briefly and let the loop expire naturally.
                        tracing::error!(
                            dependency = what,
                            "WAL dependency connect timed out repeatedly; giving up"
                        );
                        sleep(initial_backoff.to_std()).await;
                        continue;
                    };
                }
                tracing::warn!(
                    dependency = what,
                    "WAL dependency connect timed out during broker warmup; retrying"
                );
            }
        }
        sleep(backoff.to_std()).await;
        // `Time` is `PartialOrd` but not `Ord`, so `Time::min` rather than
        // `std::cmp::min`.
        backoff = (backoff * 2.0).min(max_backoff);
    }
}

pub(crate) fn validate_distributor_policy(
    config: &ServiceConfig,
) -> Result<(), ServiceConfigError> {
    if config.wal_connect_attempt_timeout > config.wal_connect_startup_deadline {
        return Err(ServiceConfigError::WalConnectAttemptExceedsDeadline);
    }
    if config.wal_connect_initial_backoff > config.wal_connect_max_backoff {
        return Err(ServiceConfigError::WalConnectInitialBackoffExceedsMaximum);
    }
    Ok(())
}

pub(crate) fn validate_compactor_policy(config: &ServiceConfig) -> Result<(), ServiceConfigError> {
    if config.compactor_accumulation_poll_timeout > config.compactor_accumulation_window {
        return Err(ServiceConfigError::CompactorAccumulationPollExceedsWindow);
    }
    if config.compactor_object_store_initial_backoff > config.compactor_object_store_max_backoff {
        return Err(ServiceConfigError::CompactorObjectStoreInitialBackoffExceedsMaximum);
    }
    Ok(())
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn build_service_dependencies(
    config: &ServiceConfig,
) -> Result<ServiceDependencies, ServiceRuntimeError> {
    build_service_dependencies_with_client_resource_policy(config, ClientResourcePolicy::default())
        .await
}

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

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn run_compactor_once(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<Option<BlockDescriptor>, ServiceRuntimeError> {
    validate_compactor_policy(config)?;
    let configured_store = build_compactor_configured_object_store(config, object_store)?;
    let (store, object_store_prefix) =
        compactor_object_store(object_store, configured_store.as_ref())?;
    let index_prefix = config
        .index_prefix
        .as_deref()
        .ok_or(ServiceConfigError::MissingCompactorIndexPrefix)?;
    let prefix = effective_object_store_prefix(object_store_prefix, index_prefix);
    let compaction_frontier = dependencies.compaction_frontier.unwrap_or_default();
    let delete_requests =
        compactor_delete_requests_for_config(config, dependencies.delete_requests)?;
    load_existing_compaction_frontier(store, &prefix, &compaction_frontier).await?;
    materialize_delete_requests_in_existing_local_manifest_blocks(
        &config.data_root,
        &delete_requests,
    )?;
    let consumer = dependencies
        .wal_consumer
        .ok_or(ServiceConfigError::MissingWalConsumer)?;
    let mut consumer = consumer.lock().await;

    let mut tenant_indexes = TenantCompactionIndexCache::new();
    let descriptors = materialize_deletes_then_compact_next_kafka_wal_batch(
        store,
        &prefix,
        consumer.as_mut(),
        config.compactor_wal_poll_timeout,
        &delete_requests,
        &mut tenant_indexes,
    )
    .await?;
    for descriptor in &descriptors {
        advance_and_persist_compaction_frontier(store, &prefix, &compaction_frontier, descriptor)
            .await?;
    }
    Ok(descriptors.into_iter().next())
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn run_compactor_until_idle(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<Vec<BlockDescriptor>, ServiceRuntimeError> {
    validate_compactor_policy(config)?;
    let configured_store = build_compactor_configured_object_store(config, object_store)?;
    let (store, object_store_prefix) =
        compactor_object_store(object_store, configured_store.as_ref())?;
    let index_prefix = config
        .index_prefix
        .as_deref()
        .ok_or(ServiceConfigError::MissingCompactorIndexPrefix)?;
    let prefix = effective_object_store_prefix(object_store_prefix, index_prefix);
    let compaction_frontier = dependencies.compaction_frontier.unwrap_or_default();
    let delete_requests =
        compactor_delete_requests_for_config(config, dependencies.delete_requests)?;
    load_existing_compaction_frontier(store, &prefix, &compaction_frontier).await?;
    materialize_delete_requests_in_existing_local_manifest_blocks(
        &config.data_root,
        &delete_requests,
    )?;
    let consumer = dependencies
        .wal_consumer
        .ok_or(ServiceConfigError::MissingWalConsumer)?;
    let mut consumer = consumer.lock().await;
    let mut descriptors = Vec::new();
    let mut tenant_indexes = TenantCompactionIndexCache::new();

    loop {
        let batch_descriptors = materialize_deletes_then_compact_next_kafka_wal_batch(
            store,
            &prefix,
            consumer.as_mut(),
            config.compactor_wal_poll_timeout,
            &delete_requests,
            &mut tenant_indexes,
        )
        .await?;
        if batch_descriptors.is_empty() {
            break;
        }
        for descriptor in batch_descriptors {
            advance_and_persist_compaction_frontier(
                store,
                &prefix,
                &compaction_frontier,
                &descriptor,
            )
            .await?;
            descriptors.push(descriptor);
        }
    }

    Ok(descriptors)
}
