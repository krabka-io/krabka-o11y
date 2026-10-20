use krabka_blockstore::{ObjectStoreAccess, ObjectStoreMetrics};
use krabka_client_core::ClientSecurity;
use krabka_units::convert::{StdDurationExt, TimeExt};

use crate::{
    BlockDescriptor, BrokerAccessPolicy, BrokerBackedIngestLimiter, ClientResourcePolicy,
    ConfiguredObjectStore, DeferredQueryAuthorizerConnect, DeferredWalConsumerConnect,
    KafkaLogWalConsumer, KafkaLogWalSink, ObjectPath, ObjectStore, Role, ServiceConfig,
    ServiceConfigError, ServiceDependencies, ServiceRuntimeError, Time,
    advance_and_persist_compaction_frontier, build_configured_object_store,
    compactor::{CompactorRunComponents, prepare_compactor_wal_inputs},
    effective_object_store_prefix, materialize_deletes_then_compact_next_kafka_wal_batch, sleep,
    wal_consumer_metrics::WalConsumerMetrics,
};

struct DeferredQueryAuthorizerDependencies {
    bootstrap: String,
    connect: DeferredQueryAuthorizerConnect,
}

pub(crate) struct WalDependenciesRequest<'a> {
    pub(crate) dependencies: ServiceDependencies,
    pub(crate) config: &'a ServiceConfig,
    pub(crate) group_id: String,
    pub(crate) client_resource_policy: ClientResourcePolicy,
    pub(crate) security: Option<&'a ClientSecurity>,
    pub(crate) metrics: WalConsumerMetrics,
}

fn deferred_query_authorizer_connect(
    config: &ServiceConfig,
    client_resource_policy: ClientResourcePolicy,
    security: Option<&ClientSecurity>,
) -> Result<DeferredQueryAuthorizerDependencies, ServiceRuntimeError> {
    let bootstrap = config
        .wal_bootstrap_server
        .as_deref()
        .ok_or(ServiceConfigError::MissingWalBootstrapServer)?
        .to_string();
    let connect = DeferredQueryAuthorizerConnect {
        bootstrap: bootstrap.clone(),
        topic: config.wal_topic.clone(),
        client_resource_policy,
        security: security.cloned(),
        access_policy: BrokerAccessPolicy::for_config(config)?,
    };
    Ok(DeferredQueryAuthorizerDependencies { bootstrap, connect })
}
#[cfg_attr(test, mutants::skip)]
/// The consumer group an all-in-one querier tails the WAL under.
///
/// The block builder keeps the configured group. The querier gets its own,
/// because a consumer group hands each member a disjoint set of partitions:
/// two members of one group would leave the block builder writing blocks from
/// half the WAL and the querier tailing the other half, and neither would say
/// so. Deriving the name rather than adding a flag keeps the all-in-one
/// configured by exactly the keys the single-role files already use.
pub(crate) fn all_in_one_querier_group_id(wal_group_id: &str) -> String {
    format!("{wal_group_id}-querier")
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
pub async fn run_compactor_once(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<Option<BlockDescriptor>, ServiceRuntimeError> {
    Ok(CompactorRun {
        config,
        dependencies,
        object_store,
    }
    .run(CompactorBatches::Next)
    .await?
    .into_iter()
    .next())
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

pub(crate) async fn build_compactor_configured_object_store(
    config: &ServiceConfig,
    object_store: Option<&dyn ObjectStore>,
    metrics: ObjectStoreMetrics,
) -> Result<Option<ConfiguredObjectStore>, ServiceConfigError> {
    if object_store.is_some() {
        return Ok(None);
    }

    build_configured_object_store(config, metrics, ObjectStoreAccess::ReadWrite).await
}

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
/// deliberately in different groups -- see `all_in_one_querier_group_id`.
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
            with_block_builder_dependencies(WalDependenciesRequest {
                dependencies,
                config,
                group_id: config.wal_group_id.clone(),
                client_resource_policy,
                security,
                metrics,
            })
            .await
        }
        Role::Querier => with_querier_dependencies(WalDependenciesRequest {
            dependencies,
            config,
            group_id: config.wal_group_id.clone(),
            client_resource_policy,
            security,
            metrics,
        }),
        Role::All => {
            let dependencies = with_distributor_dependencies(
                dependencies,
                config,
                client_resource_policy,
                security,
            )
            .await?;
            let dependencies = with_block_builder_dependencies(WalDependenciesRequest {
                dependencies,
                config,
                group_id: config.wal_group_id.clone(),
                client_resource_policy,
                security,
                metrics: metrics.clone(),
            })
            .await?;
            with_querier_dependencies(WalDependenciesRequest {
                dependencies,
                config,
                group_id: all_in_one_querier_group_id(&config.wal_group_id),
                client_resource_policy,
                security,
                metrics: metrics.with_fresh_recovery(),
            })
        }
    }
}

pub(crate) struct CompactorObjectStore<'a> {
    pub(crate) store: &'a dyn ObjectStore,
    pub(crate) prefix: ObjectPath,
}

struct ObjectStoreSelection<'a> {
    store: &'a dyn ObjectStore,
    configured_prefix: Option<&'a ObjectPath>,
}

/// Resolves the store a compactor writes to, and the index prefix under it.
///
/// An injected `object_store` wins over the configured one, and then the
/// configured store's own prefix does not apply.
pub(crate) fn compactor_object_store<'a>(
    config: &ServiceConfig,
    object_store: Option<&'a dyn ObjectStore>,
    configured_store: Option<&'a ConfiguredObjectStore>,
) -> Result<CompactorObjectStore<'a>, ServiceConfigError> {
    let selection = if let Some(store) = object_store {
        ObjectStoreSelection {
            store,
            configured_prefix: None,
        }
    } else {
        let configured_store = configured_store.ok_or(ServiceConfigError::MissingObjectStore)?;
        ObjectStoreSelection {
            store: configured_store.store.as_ref(),
            configured_prefix: Some(&configured_store.prefix),
        }
    };
    let index_prefix = config
        .index_prefix
        .as_deref()
        .ok_or(ServiceConfigError::MissingCompactorIndexPrefix)?;
    Ok(CompactorObjectStore {
        store: selection.store,
        prefix: effective_object_store_prefix(selection.configured_prefix, index_prefix),
    })
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

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn run_compactor_until_idle(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<Vec<BlockDescriptor>, ServiceRuntimeError> {
    CompactorRun {
        config,
        dependencies,
        object_store,
    }
    .run(CompactorBatches::UntilIdle)
    .await
}

/// How many WAL batches one compactor run compacts.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompactorBatches {
    /// Only the next batch, which may be empty.
    Next,
    /// Every batch, until the consumer is drained.
    UntilIdle,
}

/// What one compactor run reads its WAL and writes its blocks with.
pub(crate) struct CompactorRun<'a> {
    pub(crate) config: &'a ServiceConfig,
    pub(crate) dependencies: ServiceDependencies,
    pub(crate) object_store: Option<&'a dyn ObjectStore>,
}

impl CompactorRun<'_> {
    /// Compacts `batches` of the WAL, and advances the compaction frontier
    /// past each block it writes.
    pub(crate) async fn run(
        self,
        batches: CompactorBatches,
    ) -> Result<Vec<BlockDescriptor>, ServiceRuntimeError> {
        let Self {
            config,
            dependencies,
            object_store,
        } = self;
        validate_compactor_policy(config)?;
        let compaction_metrics = dependencies
            .compaction_metrics()
            .unwrap_or_else(crate::compaction_metrics::CompactionMetrics::unregistered);
        let configured_store = build_compactor_configured_object_store(
            config,
            object_store,
            dependencies
                .object_store_metrics()
                .unwrap_or_else(krabka_blockstore::ObjectStoreMetrics::unregistered),
        )
        .await?;
        let object_store = compactor_object_store(config, object_store, configured_store.as_ref())?;
        let store = object_store.store;
        let prefix = object_store.prefix;
        let wal_inputs =
            prepare_compactor_wal_inputs(config, &dependencies, store, &prefix).await?;
        let CompactorRunComponents {
            compaction_frontier,
            delete_requests,
            mut consumer,
            mut descriptors,
            mut tenant_indexes,
        } = wal_inputs.start().await.into_components();

        loop {
            let batch_descriptors = materialize_deletes_then_compact_next_kafka_wal_batch(
                super::runtime::NextKafkaBatch {
                    store,
                    prefix: &prefix,
                    consumer: consumer.as_mut(),
                    poll_timeout: config.compactor_wal_poll_timeout,
                    delete_requests: &delete_requests,
                    tenant_indexes: &mut tenant_indexes,
                    metrics: &compaction_metrics,
                },
            )
            .await?;
            if batches == CompactorBatches::UntilIdle
                && batch_descriptors.is_empty()
                && consumer.is_drained().await
            {
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
            if batches == CompactorBatches::Next {
                break;
            }
        }

        Ok(descriptors)
    }
}

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

/// Adds what the distributor role cannot take a push without: the WAL sink it
/// writes to, and the broker-backed limiter it checks a tenant's quota
/// against.
///
/// Both connect under `security`, the WAL client security that the service
/// loaded. `None` connects in plain text.
pub(crate) async fn with_distributor_dependencies(
    dependencies: ServiceDependencies,
    config: &ServiceConfig,
    client_resource_policy: ClientResourcePolicy,
    security: Option<&ClientSecurity>,
) -> Result<ServiceDependencies, ServiceRuntimeError> {
    validate_distributor_policy(config)?;
    let access_policy = BrokerAccessPolicy::for_config(config)?;
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
            let s = security.cloned();
            async move {
                KafkaLogWalSink::connect_with_client_resource_policy(
                    &b,
                    t,
                    client_resource_policy,
                    s,
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
                    security,
                    config.ingest_quota_burst_window,
                    access_policy,
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

/// Records what the querier's hot tail will need, without connecting it.
///
/// The connect happens in the background once the HTTP port is bound, so the
/// querier answers `/ready` with an unmet `wal-tail` gate rather than
/// answering nothing at all while the broker is still coming up. Only the
/// configuration is validated here, eagerly, so a querier pointed at no broker
/// fails at start-up rather than on its first query.
///
/// `group_id` is taken rather than read from `config` for the reason
/// [`with_block_builder_dependencies`](with_block_builder_dependencies)
/// gives: in one process these two roles must not share a consumer group.
///
/// Both deferred connects keep `security`, the WAL client security that the
/// service loaded. `None` connects in plain text.
pub(crate) fn with_querier_dependencies(
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
    Ok(dependencies
        .with_deferred_query_authorizer_connect(deferred_authorizer.connect)
        .with_deferred_wal_consumer_connect(DeferredWalConsumerConnect {
            bootstrap: deferred_authorizer.bootstrap,
            group_id,
            topic: config.wal_topic.clone(),
            client_resource_policy,
            security: security.cloned(),
            metrics: metrics.clone(),
        })
        .with_wal_recovery_metrics(metrics))
}
