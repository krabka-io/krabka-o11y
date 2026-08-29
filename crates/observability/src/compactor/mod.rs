fn build_compactor_configured_object_store(
    config: &ServiceConfig,
    object_store: Option<&dyn ObjectStore>,
) -> Result<Option<ConfiguredObjectStore>, ServiceConfigError> {
    if object_store.is_some() {
        return Ok(None);
    }

    build_configured_object_store(config)
}

fn compactor_object_store<'a>(
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
async fn connect_with_startup_retry<T, E, F, Fut>(
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

fn validate_distributor_policy(config: &ServiceConfig) -> Result<(), ServiceConfigError> {
    if config.wal_connect_attempt_timeout > config.wal_connect_startup_deadline {
        return Err(ServiceConfigError::WalConnectAttemptExceedsDeadline);
    }
    if config.wal_connect_initial_backoff > config.wal_connect_max_backoff {
        return Err(ServiceConfigError::WalConnectInitialBackoffExceedsMaximum);
    }
    Ok(())
}

fn validate_compactor_policy(config: &ServiceConfig) -> Result<(), ServiceConfigError> {
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

#[cfg_attr(test, mutants::skip)]
/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn run_compactor_until_shutdown(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
    shutdown: impl Future<Output = ()>,
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
    let mut object_store_retry_backoff = config.compactor_object_store_initial_backoff;
    let mut tenant_indexes = TenantCompactionIndexCache::new();
    let mut pending_compaction_records: Option<Vec<KafkaWalRecord>> = None;
    // Shared RED-metrics bundle for the `:9404` exporter; `None` in tests that
    // don't wire metrics, so block-written accounting is a no-op there.
    let metrics = dependencies.metrics.clone();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            biased;
            () = &mut shutdown => return Ok(descriptors),
            () = sleep(<Time as TimeExt>::ZERO.to_std()) => {}
        }

        let batch_result = match materialize_log_deletes_before_compaction(
            store,
            &prefix,
            &delete_requests,
            &mut tenant_indexes,
        )
        .await
        {
            Ok(()) => {
                let records = match pending_compaction_records.take() {
                    Some(records) => records,
                    None => {
                        match poll_accumulated_log_compaction_records(
                            consumer.as_mut(),
                            config.compactor_wal_poll_timeout,
                            config.compactor_accumulation_window,
                            config.compactor_accumulation_poll_timeout,
                            config.compactor_max_records_per_batch,
                        )
                        .await
                        {
                            Ok(records) => records,
                            Err(error) => return Err(CompactorRunError::from(error).into()),
                        }
                    }
                };
                if records.is_empty() {
                    Ok(Vec::new())
                } else {
                    let retry_records = records.clone();
                    match compact_polled_kafka_wal_records_to_object_store_from_existing_manifest(
                        store,
                        &prefix,
                        consumer.as_mut(),
                        records,
                        &delete_requests,
                        &mut tenant_indexes,
                    )
                    .await
                    {
                        Ok(batch_descriptors) => Ok(batch_descriptors),
                        Err(error) => {
                            pending_compaction_records = Some(retry_records);
                            Err(error)
                        }
                    }
                }
            }
            Err(error) => Err(error),
        };
        let batch_descriptors = match batch_result {
            Ok(batch_descriptors) => {
                object_store_retry_backoff = config.compactor_object_store_initial_backoff;
                batch_descriptors
            }
            Err(error) if compactor_run_error_is_object_store(&error) => {
                tenant_indexes.clear();
                tokio::select! {
                    () = &mut shutdown => return Ok(descriptors),
                    () = sleep(object_store_retry_backoff.to_std()) => {}
                }
                object_store_retry_backoff = next_compactor_object_store_backoff(
                    object_store_retry_backoff,
                    config.compactor_object_store_max_backoff,
                );
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        if batch_descriptors.is_empty() {
            tokio::select! {
                () = &mut shutdown => return Ok(descriptors),
                () = sleep(config.compactor_idle_interval.to_std()) => {}
            }
        } else {
            for descriptor in batch_descriptors {
                loop {
                    match advance_and_persist_compaction_frontier(
                        store,
                        &prefix,
                        &compaction_frontier,
                        &descriptor,
                    )
                    .await
                    {
                        Ok(()) => {
                            object_store_retry_backoff =
                                config.compactor_object_store_initial_backoff;
                            // One durable log block persisted to object storage.
                            if let Some(metrics) = &metrics {
                                metrics.record_block_written();
                            }
                            break;
                        }
                        Err(error) if compactor_run_error_is_object_store(&error) => {
                            tokio::select! {
                                () = &mut shutdown => return Err(error.into()),
                                () = sleep(object_store_retry_backoff.to_std()) => {}
                            }
                            object_store_retry_backoff = next_compactor_object_store_backoff(
                                object_store_retry_backoff,
                                config.compactor_object_store_max_backoff,
                            );
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                descriptors.push(descriptor);
            }
        }
    }
}

/// Doubles the object-store retry backoff, up to a cap.
///
/// `Time` is `PartialOrd` but not `Ord`, so this uses `Time::min` and not
/// `std::cmp::min`.
fn next_compactor_object_store_backoff(current: Time, max_backoff: Time) -> Time {
    (current * 2.0).min(max_backoff)
}

fn compactor_run_error_is_object_store(error: &CompactorRunError) -> bool {
    match error {
        CompactorRunError::Wal(KafkaWalCompactionError::Compaction(error))
        | CompactorRunError::Compaction(error) => compaction_error_is_object_store(error),
        CompactorRunError::BlockStore(error) => block_store_error_is_object_store(error),
        CompactorRunError::Frontier(CompactionFrontierStoreError::ObjectStore(_)) => true,
        CompactorRunError::Wal(KafkaWalCompactionError::Decode(_))
        | CompactorRunError::Decode(_)
        | CompactorRunError::Consumer(_)
        | CompactorRunError::Frontier(
            CompactionFrontierStoreError::InvalidVersion { .. }
            | CompactionFrontierStoreError::Json(_),
        )
        | CompactorRunError::DeleteFilter(_)
        | CompactorRunError::MissingSeriesLabels { .. }
        | CompactorRunError::MissingCommitPosition => false,
    }
}

fn compaction_error_is_object_store(error: &CompactionError) -> bool {
    match error {
        CompactionError::BlockStore(error) => block_store_error_is_object_store(error),
        CompactionError::EmptyWalBatch
        | CompactionError::AllRowsDeleted
        | CompactionError::MissingWalPosition { .. }
        | CompactionError::MixedTenant { .. }
        | CompactionError::MixedPartition { .. }
        | CompactionError::Commit(_) => false,
    }
}

fn block_store_error_is_object_store(error: &BlockStoreError) -> bool {
    matches!(error, BlockStoreError::ObjectStore(_))
}

async fn load_existing_compaction_frontier(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    frontier: &SharedCompactionFrontier,
) -> Result<(), CompactionFrontierStoreError> {
    match read_compaction_frontier_from_object_store(store, prefix).await {
        Ok(loaded) => frontier.replace(loaded),
        Err(CompactionFrontierStoreError::ObjectStore(object_store::Error::NotFound {
            ..
        })) => {}
        Err(error) => return Err(error),
    }
    Ok(())
}

async fn shared_compaction_frontier_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> Result<SharedCompactionFrontier, CompactionFrontierStoreError> {
    let frontier = SharedCompactionFrontier::default();
    load_existing_compaction_frontier(store, prefix, &frontier).await?;
    Ok(frontier)
}

async fn refresh_compaction_frontier_and_prune(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    frontier: &SharedCompactionFrontier,
    hot_tail: &BufferedLogHotTail,
) -> Result<usize, CompactionFrontierStoreError> {
    let updated = match read_compaction_frontier_from_object_store(store, prefix).await {
        Ok(updated) => updated,
        Err(CompactionFrontierStoreError::ObjectStore(object_store::Error::NotFound {
            ..
        })) => return Ok(0),
        Err(error) => return Err(error),
    };
    frontier.replace(updated.clone());
    Ok(hot_tail.prune_compacted(&updated))
}

#[cfg_attr(test, mutants::skip)]
fn spawn_compaction_frontier_refresher(
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    frontier: SharedCompactionFrontier,
    hot_tail: BufferedLogHotTail,
    token: CancellationToken,
    refresh_interval: Time,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = token.cancelled() => return,
                () = sleep(refresh_interval.to_std()) => {}
            }

            if let Err(error) =
                refresh_compaction_frontier_and_prune(store.as_ref(), &prefix, &frontier, &hot_tail)
                    .await
            {
                tracing::warn!(%error, "compaction frontier refresh failed; retaining last good frontier");
            }
        }
    });
}

async fn advance_and_persist_compaction_frontier(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    frontier: &SharedCompactionFrontier,
    descriptor: &BlockDescriptor,
) -> Result<(), CompactorRunError> {
    frontier.advance_partition_offset(WalPosition {
        partition: PartitionIndex(descriptor.key.partition),
        offset: Offset(descriptor.key.last_offset),
    });
    write_compaction_frontier_to_object_store(store, prefix, &frontier.snapshot()).await?;
    Ok(())
}

async fn materialize_deletes_then_compact_next_kafka_wal_batch(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    consumer: &mut (impl LogWalConsumer + ?Sized),
    poll_timeout: Time,
    delete_requests: &SharedLogDeleteRequests,
    tenant_indexes: &mut TenantCompactionIndexCache,
) -> Result<Vec<BlockDescriptor>, CompactorRunError> {
    materialize_log_deletes_before_compaction(store, prefix, delete_requests, tenant_indexes)
        .await?;
    compact_next_kafka_wal_batch_to_object_store_from_existing_manifest(
        store,
        prefix,
        consumer,
        poll_timeout,
        delete_requests,
        tenant_indexes,
    )
    .await
}

async fn compact_next_kafka_wal_batch_to_object_store_from_existing_manifest(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    consumer: &mut (impl LogWalConsumer + ?Sized),
    poll_timeout: Time,
    delete_requests: &SharedLogDeleteRequests,
    tenant_indexes: &mut TenantCompactionIndexCache,
) -> Result<Vec<BlockDescriptor>, CompactorRunError> {
    let records = consumer.poll(poll_timeout).await?;
    compact_polled_kafka_wal_records_to_object_store_from_existing_manifest(
        store,
        prefix,
        consumer,
        records,
        delete_requests,
        tenant_indexes,
    )
    .await
}

async fn materialize_log_deletes_before_compaction(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    delete_requests: &SharedLogDeleteRequests,
    tenant_indexes: &mut TenantCompactionIndexCache,
) -> Result<(), CompactorRunError> {
    if !active_log_delete_tenants(delete_requests)?.is_empty() {
        materialize_delete_requests_in_existing_object_store_blocks(store, prefix, delete_requests)
            .await?;
        tenant_indexes.clear();
    }
    Ok(())
}

/// Re-parents `span` into the trace carried by the first record whose headers
/// include a `traceparent`.
///
/// There is one span per poll batch rather than one per record, so the first
/// record carrying a trace context stands for the batch. A record without one
/// is skipped rather than used: extracting from its headers would find no
/// context and leave the batch in a trace of its own.
fn set_remote_parent_from_wal_records(span: &tracing::Span, records: &[KafkaWalRecord]) {
    let Some(parent) = records
        .iter()
        .find(|rec| rec.headers.iter().any(|h| h.key == "traceparent"))
    else {
        return;
    };
    krabka_telemetry::propagation::set_remote_parent(
        span,
        parent
            .headers
            .iter()
            .map(|h| (h.key.as_str(), h.value.as_deref().unwrap_or(&[][..]))),
    );
}

async fn compact_polled_kafka_wal_records_to_object_store_from_existing_manifest(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    consumer: &mut (impl LogWalConsumer + ?Sized),
    records: Vec<KafkaWalRecord>,
    delete_requests: &SharedLogDeleteRequests,
    tenant_indexes: &mut TenantCompactionIndexCache,
) -> Result<Vec<BlockDescriptor>, CompactorRunError> {
    if records.is_empty() {
        return Ok(Vec::new());
    }

    // ONE consumer span per poll batch: stitch it onto the ingest trace via the
    // `traceparent` a producer injected in `build_kafka_wal_record`. The first
    // record carrying a trace context is representative of the batch.
    let span = tracing::info_span!(
        "logs_compaction",
        otel.kind = "consumer",
        krabka.wal.records = records.len(),
    );
    set_remote_parent_from_wal_records(&span, &records);

    compact_polled_kafka_wal_records_inner(
        store,
        prefix,
        consumer,
        records,
        delete_requests,
        tenant_indexes,
    )
    .instrument(span)
    .await
}

async fn compact_polled_kafka_wal_records_inner(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    consumer: &mut (impl LogWalConsumer + ?Sized),
    records: Vec<KafkaWalRecord>,
    delete_requests: &SharedLogDeleteRequests,
    tenant_indexes: &mut TenantCompactionIndexCache,
) -> Result<Vec<BlockDescriptor>, CompactorRunError> {
    let decoded = records
        .into_iter()
        .map(decode_kafka_wal_record_envelope)
        .collect::<Result<Vec<_>, _>>()?;
    let mut descriptors = Vec::new();
    let mut commit_positions: BTreeMap<PartitionIndex, Offset> = BTreeMap::new();

    for chunk in wal_compaction_chunks(decoded) {
        let tenant = chunk
            .first()
            .ok_or(CompactionError::EmptyWalBatch)?
            .tenant
            .clone();
        let (label_index, block_index) = tenant_indexes
            .entry(tenant.clone())
            .or_insert_with(|| (LabelIndex::default(), BlockIndex::default()));
        let mut committer = LastCompactedPosition::default();
        let time_range = wal_record_time_range(&chunk)?;
        let delete_filters =
            active_log_delete_filters_from_requests(delete_requests, &tenant, time_range)?;
        let descriptor = compact_wal_records_to_object_store_with_delete_filters_and_index_output(
            store,
            prefix,
            label_index,
            block_index,
            &mut committer,
            chunk,
            (&delete_filters, LogCompactionIndexOutput::ShardManifests),
        )
        .await?;
        let position = committer
            .position
            .ok_or(CompactorRunError::MissingCommitPosition)?;
        commit_positions
            .entry(position.partition)
            .and_modify(|offset| *offset = (*offset).max(position.offset))
            .or_insert(position.offset);
        if let Some(descriptor) = descriptor {
            descriptors.push(descriptor);
        }
    }

    for (partition, offset) in commit_positions {
        consumer
            .commit_compacted(WalPosition { partition, offset })
            .await?;
    }

    Ok(descriptors)
}

async fn poll_accumulated_log_compaction_records(
    consumer: &mut (impl LogWalConsumer + ?Sized),
    initial_timeout: Time,
    accumulation_window: Time,
    accumulation_poll_timeout: Time,
    max_records_per_batch: NonZeroUsize,
) -> Result<Vec<KafkaWalRecord>, WalConsumerError> {
    let mut records = consumer.poll(initial_timeout).await?;
    if records.is_empty() || records.len() >= max_records_per_batch.get() {
        return Ok(records);
    }

    let deadline = Instant::now() + accumulation_window.to_std();
    while records.len() < max_records_per_batch.get() {
        let remaining = deadline.saturating_duration_since(Instant::now()).as_time();
        if remaining <= <Time as TimeExt>::ZERO {
            break;
        }

        // `Time` is `PartialOrd` but not `Ord`, so `Time::min` rather than
        // `std::cmp::min`.
        let poll_timeout = remaining.min(accumulation_poll_timeout);
        let next = consumer.poll(poll_timeout).await?;
        if next.is_empty() {
            break;
        }
        records.extend(next);
    }

    Ok(records)
}

async fn materialize_delete_requests_in_existing_object_store_blocks(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    delete_requests: &SharedLogDeleteRequests,
) -> Result<(), CompactorRunError> {
    for tenant in active_log_delete_tenants(delete_requests)? {
        let mut materialized_blocks: BTreeMap<String, Option<BlockDescriptor>> = BTreeMap::new();

        match read_tenant_log_index_manifest_from_object_store(store, prefix, &tenant).await {
            Ok((label_index, block_index)) => {
                if let Some((next_label_index, next_block_index)) =
                    materialize_delete_requests_in_object_store_block_index(
                        store,
                        prefix,
                        &tenant,
                        &label_index,
                        &block_index,
                        delete_requests,
                        &mut materialized_blocks,
                    )
                    .await?
                {
                    write_tenant_log_index_manifest_to_object_store(
                        store,
                        prefix,
                        &tenant,
                        &next_label_index,
                        &next_block_index,
                    )
                    .await?;
                }
            }
            Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => {}
            Err(error) => return Err(error.into()),
        }

        let shard_ranges = match read_tenant_log_index_shard_ranges_from_object_store(
            store, prefix, &tenant,
        )
        .await
        {
            Ok(shard_ranges) => shard_ranges,
            Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => {
                continue;
            }
            Err(error) => return Err(error.into()),
        };

        for shard_range in shard_ranges {
            let (label_index, block_index) =
                read_tenant_log_index_shard_from_object_store(store, prefix, &tenant, shard_range)
                    .await?;
            if let Some((next_label_index, next_block_index)) =
                materialize_delete_requests_in_object_store_block_index(
                    store,
                    prefix,
                    &tenant,
                    &label_index,
                    &block_index,
                    delete_requests,
                    &mut materialized_blocks,
                )
                .await?
            {
                write_tenant_log_index_shard_to_object_store(
                    store,
                    prefix,
                    &tenant,
                    shard_range,
                    &next_label_index,
                    &next_block_index,
                )
                .await?;
            }
        }
    }
    Ok(())
}

#[cfg_attr(test, mutants::skip)]
fn materialize_delete_requests_in_existing_local_manifest_blocks(
    root: &FsPath,
    delete_requests: &SharedLogDeleteRequests,
) -> Result<(), CompactorRunError> {
    let (label_index, block_index) = match read_log_index_manifest(root) {
        Ok(indexes) => indexes,
        Err(BlockStoreError::Io(error)) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };

    let active_tenants = active_log_delete_tenants(delete_requests)?;
    if active_tenants.is_empty() {
        return Ok(());
    }

    let mut next_label_index = LabelIndex::default();
    let mut next_block_index = BlockIndex::default();
    let mut changed = false;

    for block in block_index.blocks() {
        let tenant = &block.key.tenant;
        let delete_filters = if active_tenants.contains(tenant) {
            active_log_delete_filters_from_requests(delete_requests, tenant, block.key.time_range)?
        } else {
            Vec::new()
        };
        let mut descriptor = block.clone();

        if !delete_filters.is_empty() {
            let rows = read_log_block(root, &block.key)?;
            let original_len = rows.len();
            let mut kept_rows = Vec::with_capacity(original_len);
            for row in rows {
                let labels = label_index
                    .labels_for(tenant, row.series_fingerprint)
                    .ok_or_else(|| CompactorRunError::MissingSeriesLabels {
                        tenant: tenant.clone(),
                        fingerprint: row.series_fingerprint,
                    })?;
                if is_deleted_log_entry(
                    &delete_filters,
                    labels,
                    &row.line,
                    &row.structured_metadata,
                    row.timestamp_ns,
                ) {
                    continue;
                }
                kept_rows.push(row);
            }

            if kept_rows.len() != original_len {
                changed = true;
                if kept_rows.is_empty() {
                    continue;
                }
                descriptor = write_log_block(root, &block.key, kept_rows)?;
            }
        }

        insert_descriptor_labels(&mut next_label_index, &label_index, tenant, &descriptor)?;
        next_block_index.insert(descriptor);
    }

    if changed {
        write_log_index_manifest(root, &next_label_index, &next_block_index)?;
    }
    Ok(())
}

#[cfg_attr(test, mutants::skip)]
async fn materialize_delete_requests_in_object_store_block_index(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
    delete_requests: &SharedLogDeleteRequests,
    materialized_blocks: &mut BTreeMap<String, Option<BlockDescriptor>>,
) -> Result<Option<(LabelIndex, BlockIndex)>, CompactorRunError> {
    let mut next_label_index = LabelIndex::default();
    let mut next_block_index = BlockIndex::default();
    let mut changed = false;

    for block in block_index.blocks() {
        let object_key = block.key.object_key();
        if let Some(materialized) = materialized_blocks.get(&object_key) {
            match materialized {
                Some(descriptor) => {
                    if descriptor != block {
                        changed = true;
                    }
                    insert_descriptor_labels(
                        &mut next_label_index,
                        label_index,
                        tenant,
                        descriptor,
                    )?;
                    next_block_index.insert(descriptor.clone());
                }
                None => {
                    changed = true;
                }
            }
            continue;
        }

        let delete_filters =
            active_log_delete_filters_from_requests(delete_requests, tenant, block.key.time_range)?;
        let mut descriptor = block.clone();

        if delete_filters.is_empty() {
            insert_descriptor_labels(&mut next_label_index, label_index, tenant, &descriptor)?;
            next_block_index.insert(descriptor);
            continue;
        }

        let rows = read_log_block_from_object_store(store, prefix, &block.key).await?;
        let original_len = rows.len();
        let mut kept_rows = Vec::with_capacity(original_len);
        for row in rows {
            let labels = label_index
                .labels_for(tenant, row.series_fingerprint)
                .ok_or_else(|| CompactorRunError::MissingSeriesLabels {
                    tenant: tenant.to_string(),
                    fingerprint: row.series_fingerprint,
                })?;
            if is_deleted_log_entry(
                &delete_filters,
                labels,
                &row.line,
                &row.structured_metadata,
                row.timestamp_ns,
            ) {
                continue;
            }
            kept_rows.push(row);
        }

        if kept_rows.len() != original_len {
            changed = true;
            if kept_rows.is_empty() {
                materialized_blocks.insert(object_key, None);
                continue;
            }
            descriptor =
                write_log_block_to_object_store(store, prefix, &block.key, kept_rows).await?;
            materialized_blocks.insert(object_key, Some(descriptor.clone()));
        }

        insert_descriptor_labels(&mut next_label_index, label_index, tenant, &descriptor)?;
        next_block_index.insert(descriptor);
    }

    Ok(changed.then_some((next_label_index, next_block_index)))
}

fn active_log_delete_tenants(
    delete_requests: &SharedLogDeleteRequests,
) -> Result<BTreeSet<String>, ActiveLogDeleteFilterError> {
    delete_requests.refresh()?;
    let requests = delete_requests
        .inner
        .lock()
        .expect("compactor delete state poisoned");
    Ok(requests
        .requests
        .iter()
        .map(|request| request.tenant.clone())
        .collect())
}

fn insert_descriptor_labels(
    target: &mut LabelIndex,
    source: &LabelIndex,
    tenant: &str,
    descriptor: &BlockDescriptor,
) -> Result<(), CompactorRunError> {
    for fingerprint in &descriptor.fingerprints {
        let labels = source.labels_for(tenant, *fingerprint).ok_or_else(|| {
            CompactorRunError::MissingSeriesLabels {
                tenant: tenant.to_string(),
                fingerprint: *fingerprint,
            }
        })?;
        target.insert_series(tenant.to_string(), labels.clone());
    }
    Ok(())
}

fn wal_compaction_chunks(records: Vec<WalLogRecord>) -> Vec<Vec<WalLogRecord>> {
    let mut chunks: Vec<Vec<WalLogRecord>> = Vec::new();
    for record in records {
        let Some(position) = record.position else {
            chunks.push(vec![record]);
            continue;
        };
        if let Some(chunk) = chunks.last_mut()
            && chunk.first().is_some_and(|first| {
                first.tenant == record.tenant
                    && first.position.is_some_and(|first_position| {
                        first_position.partition == position.partition
                    })
            })
        {
            chunk.push(record);
        } else {
            chunks.push(vec![record]);
        }
    }
    chunks
}

fn wal_record_time_range(records: &[WalLogRecord]) -> Result<TimeRange, CompactionError> {
    let first = records.first().ok_or(CompactionError::EmptyWalBatch)?;
    let mut start_ns = first.timestamp_ns;
    let mut end_ns = first.timestamp_ns;
    for record in records.iter().skip(1) {
        start_ns = start_ns.min(record.timestamp_ns);
        end_ns = end_ns.max(record.timestamp_ns);
    }
    Ok(TimeRange::new(start_ns, end_ns)?)
}

type TenantCompactionIndexCache = BTreeMap<String, (LabelIndex, BlockIndex)>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LogCompactionIndexOutput {
    FullManifestAndShardCatalog,
    ShardManifests,
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn compact_log_block_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    key: &BlockKey,
    label_index: &LabelIndex,
    block_index: &mut BlockIndex,
    rows: Vec<LogRow>,
) -> Result<BlockDescriptor, BlockStoreError> {
    compact_log_block_to_object_store_with_index_output(
        store,
        prefix,
        key,
        label_index,
        block_index,
        rows,
        LogCompactionIndexOutput::FullManifestAndShardCatalog,
    )
    .await
}

async fn compact_log_block_to_object_store_with_index_output(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    key: &BlockKey,
    label_index: &LabelIndex,
    block_index: &mut BlockIndex,
    rows: Vec<LogRow>,
    index_output: LogCompactionIndexOutput,
) -> Result<BlockDescriptor, BlockStoreError> {
    let descriptor = write_log_block_to_object_store(store, prefix, key, rows).await?;
    block_index.insert(descriptor.clone());
    write_tenant_compaction_indexes_to_object_store(
        store,
        prefix,
        &key.tenant,
        &descriptor,
        label_index,
        block_index,
        index_output,
    )
    .await?;
    Ok(descriptor)
}

async fn write_tenant_compaction_indexes_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    new_descriptor: &BlockDescriptor,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
    index_output: LogCompactionIndexOutput,
) -> Result<(), BlockStoreError> {
    if index_output == LogCompactionIndexOutput::ShardManifests {
        let mut shard_block_index = BlockIndex::default();
        shard_block_index.insert(new_descriptor.clone());
        write_tenant_log_index_shard_to_object_store(
            store,
            prefix,
            tenant,
            new_descriptor.key.time_range,
            label_index,
            &shard_block_index,
        )
        .await?;
        return Ok(());
    }

    if index_output == LogCompactionIndexOutput::FullManifestAndShardCatalog {
        write_tenant_log_index_manifest_to_object_store(
            store,
            prefix,
            tenant,
            label_index,
            block_index,
        )
        .await?;
    }

    write_tenant_log_index_shard_to_object_store(
        store,
        prefix,
        tenant,
        new_descriptor.key.time_range,
        label_index,
        block_index,
    )
    .await?;

    let mut shard_ranges =
        match read_tenant_log_index_shard_ranges_from_object_store(store, prefix, tenant).await {
            Ok(shard_ranges) => shard_ranges,
            Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => Vec::new(),
            Err(error) => return Err(error),
        };
    if !shard_ranges.contains(&new_descriptor.key.time_range) {
        shard_ranges.push(new_descriptor.key.time_range);
    }
    shard_ranges.sort_by_key(|range| (range.start_ns, range.end_ns));
    write_tenant_log_index_shard_catalog_to_object_store(store, prefix, tenant, &shard_ranges).await
}

pub trait CompactionOffsetCommitter {
    /// # Errors
    /// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
    fn commit_compacted(&mut self, position: WalPosition) -> Result<(), CompactionCommitError>;
}

#[derive(Debug, Error)]
#[error("offset commit failed")]
pub struct CompactionCommitError;

#[derive(Debug, Error)]
pub enum CompactionError {
    #[error("cannot compact an empty WAL batch")]
    EmptyWalBatch,
    #[error("cannot compact WAL batch after all rows were deleted")]
    AllRowsDeleted,
    #[error("missing WAL position for record at timestamp {timestamp_ns}")]
    MissingWalPosition { timestamp_ns: i64 },
    #[error("cannot compact mixed-tenant WAL batch: expected {expected}, got {actual}")]
    MixedTenant { expected: String, actual: String },
    #[error("cannot compact mixed-partition WAL batch: expected {expected}, got {actual}")]
    MixedPartition { expected: i32, actual: i32 },
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error(transparent)]
    Commit(#[from] CompactionCommitError),
}

#[derive(Debug, Error)]
pub enum KafkaWalCompactionError {
    #[error(transparent)]
    Decode(#[from] WalRecordDecodeError),
    #[error(transparent)]
    Compaction(#[from] CompactionError),
}

#[derive(Debug, Error)]
pub enum CompactorRunError {
    #[error(transparent)]
    Wal(#[from] KafkaWalCompactionError),
    #[error(transparent)]
    Decode(#[from] WalRecordDecodeError),
    #[error(transparent)]
    Compaction(#[from] CompactionError),
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error(transparent)]
    Consumer(#[from] WalConsumerError),
    #[error(transparent)]
    Frontier(#[from] CompactionFrontierStoreError),
    #[error(transparent)]
    DeleteFilter(#[from] ActiveLogDeleteFilterError),
    #[error("missing labels for tenant `{tenant}` series fingerprint {fingerprint}")]
    MissingSeriesLabels {
        tenant: String,
        fingerprint: SeriesFingerprint,
    },
    #[error("compacted WAL batch did not report a commit position")]
    MissingCommitPosition,
}

#[derive(Debug, Error)]
pub enum CompactionFrontierStoreError {
    #[error("invalid compaction frontier manifest version {actual}; expected {expected}")]
    InvalidVersion { actual: u32, expected: u32 },
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn compact_next_kafka_wal_batch_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    label_index: &mut LabelIndex,
    block_index: &mut BlockIndex,
    consumer: &mut (impl LogWalConsumer + ?Sized),
    poll_timeout: Time,
) -> Result<Option<BlockDescriptor>, CompactorRunError> {
    let records = consumer.poll(poll_timeout).await?;
    if records.is_empty() {
        return Ok(None);
    }

    let mut committer = LastCompactedPosition::default();
    let descriptor = compact_kafka_wal_records_to_object_store(
        store,
        prefix,
        label_index,
        block_index,
        &mut committer,
        records,
    )
    .await?;
    let position = committer
        .position
        .ok_or(CompactorRunError::MissingCommitPosition)?;
    consumer.commit_compacted(position).await?;

    Ok(Some(descriptor))
}

#[derive(Default)]
struct LastCompactedPosition {
    position: Option<WalPosition>,
}

impl CompactionOffsetCommitter for LastCompactedPosition {
    fn commit_compacted(&mut self, position: WalPosition) -> Result<(), CompactionCommitError> {
        self.position = Some(position);
        Ok(())
    }
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn compact_kafka_wal_records_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    label_index: &mut LabelIndex,
    block_index: &mut BlockIndex,
    committer: &mut impl CompactionOffsetCommitter,
    records: Vec<KafkaWalRecord>,
) -> Result<BlockDescriptor, KafkaWalCompactionError> {
    let decoded = records
        .into_iter()
        .map(decode_kafka_wal_record_envelope)
        .collect::<Result<Vec<_>, _>>()?;

    Ok(compact_wal_records_to_object_store(
        store,
        prefix,
        label_index,
        block_index,
        committer,
        decoded,
    )
    .await?)
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn compact_wal_records_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    label_index: &mut LabelIndex,
    block_index: &mut BlockIndex,
    committer: &mut impl CompactionOffsetCommitter,
    records: Vec<WalLogRecord>,
) -> Result<BlockDescriptor, CompactionError> {
    compact_wal_records_to_object_store_with_delete_filters_and_index_output(
        store,
        prefix,
        label_index,
        block_index,
        committer,
        records,
        (&[], LogCompactionIndexOutput::FullManifestAndShardCatalog),
    )
    .await?
    .ok_or(CompactionError::AllRowsDeleted)
}

async fn compact_wal_records_to_object_store_with_delete_filters_and_index_output(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    label_index: &mut LabelIndex,
    block_index: &mut BlockIndex,
    committer: &mut impl CompactionOffsetCommitter,
    records: Vec<WalLogRecord>,
    output: (&[ActiveLogDeleteFilter], LogCompactionIndexOutput),
) -> Result<Option<BlockDescriptor>, CompactionError> {
    let (delete_filters, index_output) = output;
    let first = records.first().ok_or(CompactionError::EmptyWalBatch)?;
    let tenant = first.tenant.clone();
    let first_position = first.position.ok_or(CompactionError::MissingWalPosition {
        timestamp_ns: first.timestamp_ns,
    })?;
    let partition = first_position.partition;
    let mut first_offset = first_position.offset;
    let mut last_offset = first_position.offset;
    let mut start_ns = first.timestamp_ns;
    let mut end_ns = first.timestamp_ns;
    let mut staged_label_index = label_index.clone();
    let mut rows = Vec::with_capacity(records.len());

    for record in records {
        if record.tenant != tenant {
            return Err(CompactionError::MixedTenant {
                expected: tenant,
                actual: record.tenant,
            });
        }
        let position = record.position.ok_or(CompactionError::MissingWalPosition {
            timestamp_ns: record.timestamp_ns,
        })?;
        if position.partition != partition {
            return Err(CompactionError::MixedPartition {
                expected: partition.get(),
                actual: position.partition.get(),
            });
        }

        first_offset = first_offset.min(position.offset);
        last_offset = last_offset.max(position.offset);
        start_ns = start_ns.min(record.timestamp_ns);
        end_ns = end_ns.max(record.timestamp_ns);
        if is_deleted_log_entry(
            delete_filters,
            &record.labels,
            &record.line,
            &record.structured_metadata,
            record.timestamp_ns,
        ) {
            continue;
        }
        let fingerprint = staged_label_index.insert_series(&tenant, record.labels);
        rows.push(LogRow::new(
            fingerprint,
            record.timestamp_ns,
            record.line,
            record.structured_metadata,
        ));
    }

    if rows.is_empty() {
        committer.commit_compacted(WalPosition {
            partition,
            offset: last_offset,
        })?;
        return Ok(None);
    }

    let key = BlockKey::new(
        tenant,
        partition.get(),
        first_offset.get(),
        last_offset.get(),
        TimeRange::new(start_ns, end_ns)?,
    );
    let mut staged_block_index = block_index.clone();
    let descriptor = compact_log_block_to_object_store_with_index_output(
        store,
        prefix,
        &key,
        &staged_label_index,
        &mut staged_block_index,
        rows,
        index_output,
    )
    .await?;

    committer.commit_compacted(WalPosition {
        partition,
        offset: last_offset,
    })?;
    *label_index = staged_label_index;
    *block_index = staged_block_index;

    Ok(Some(descriptor))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WalPosition {
    pub partition: PartitionIndex,
    pub offset: Offset,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WalLogRecord {
    pub tenant: String,
    pub labels: Labels,
    pub timestamp_ns: i64,
    pub line: String,
    pub structured_metadata: BTreeMap<String, String>,
    pub position: Option<WalPosition>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KafkaWalRecord {
    pub value: Vec<u8>,
    pub partition: PartitionIndex,
    pub offset: Offset,
    pub timestamp_ms: Option<i64>,
    pub headers: Vec<KafkaWalHeader>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KafkaWalHeader {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionFrontier {
    pub compacted_through_ns: i64,
    partition_offsets: BTreeMap<PartitionIndex, Offset>,
}

impl CompactionFrontier {
    #[must_use]
    pub fn new(compacted_through_ns: i64) -> Self {
        Self {
            compacted_through_ns,
            partition_offsets: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn with_partition_offset(mut self, partition: PartitionIndex, offset: Offset) -> Self {
        self.partition_offsets.insert(partition, offset);
        self
    }

    pub fn advance_partition_offset(&mut self, position: WalPosition) {
        self.partition_offsets
            .entry(position.partition)
            .and_modify(|offset| *offset = (*offset).max(position.offset))
            .or_insert(position.offset);
    }

    fn is_compacted(&self, record: &WalLogRecord) -> bool {
        if let Some(position) = record.position
            && self
                .partition_offsets
                .get(&position.partition)
                .is_some_and(|offset| position.offset <= *offset)
        {
            return true;
        }

        record.timestamp_ns <= self.compacted_through_ns
    }
}

#[derive(Clone, Debug)]
pub struct SharedCompactionFrontier {
    frontier: Arc<Mutex<CompactionFrontier>>,
}

impl SharedCompactionFrontier {
    #[must_use]
    pub fn new(frontier: CompactionFrontier) -> Self {
        Self {
            frontier: Arc::new(Mutex::new(frontier)),
        }
    }

    #[must_use]
    /// # Panics
    /// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
    pub fn snapshot(&self) -> CompactionFrontier {
        self.frontier
            .lock()
            .expect("frontier mutex poisoned")
            .clone()
    }

    /// # Panics
    /// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
    pub fn advance_partition_offset(&self, position: WalPosition) {
        self.frontier
            .lock()
            .expect("frontier mutex poisoned")
            .advance_partition_offset(position);
    }

    /// # Panics
    /// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
    pub fn replace(&self, frontier: CompactionFrontier) {
        *self.frontier.lock().expect("frontier mutex poisoned") = frontier;
    }
}

impl Default for SharedCompactionFrontier {
    fn default() -> Self {
        Self::new(CompactionFrontier::new(i64::MIN))
    }
}

const COMPACTION_FRONTIER_MANIFEST_VERSION: u32 = 1;
const COMPACTION_FRONTIER_MANIFEST_RELATIVE_PATH: &str = "index/logs/compaction-frontier.json";

#[derive(Deserialize, Serialize)]
struct CompactionFrontierManifest {
    version: u32,
    compacted_through_ns: i64,
    partition_offsets: BTreeMap<PartitionIndex, Offset>,
}

impl From<&CompactionFrontier> for CompactionFrontierManifest {
    fn from(frontier: &CompactionFrontier) -> Self {
        Self {
            version: COMPACTION_FRONTIER_MANIFEST_VERSION,
            compacted_through_ns: frontier.compacted_through_ns,
            partition_offsets: frontier.partition_offsets.clone(),
        }
    }
}

impl TryFrom<CompactionFrontierManifest> for CompactionFrontier {
    type Error = CompactionFrontierStoreError;

    fn try_from(manifest: CompactionFrontierManifest) -> Result<Self, Self::Error> {
        if manifest.version != COMPACTION_FRONTIER_MANIFEST_VERSION {
            return Err(CompactionFrontierStoreError::InvalidVersion {
                actual: manifest.version,
                expected: COMPACTION_FRONTIER_MANIFEST_VERSION,
            });
        }

        Ok(Self {
            compacted_through_ns: manifest.compacted_through_ns,
            partition_offsets: manifest.partition_offsets,
        })
    }
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn write_compaction_frontier_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    frontier: &CompactionFrontier,
) -> Result<(), CompactionFrontierStoreError> {
    let payload = serde_json::to_vec_pretty(&CompactionFrontierManifest::from(frontier))?;
    store
        .put(
            &compaction_frontier_manifest_object_path(prefix),
            payload.into(),
        )
        .await?;
    Ok(())
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn read_compaction_frontier_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> Result<CompactionFrontier, CompactionFrontierStoreError> {
    let bytes = store
        .get(&compaction_frontier_manifest_object_path(prefix))
        .await?
        .bytes()
        .await?;
    let manifest: CompactionFrontierManifest = serde_json::from_slice(&bytes)?;
    manifest.try_into()
}

fn compaction_frontier_manifest_object_path(prefix: &ObjectPath) -> ObjectPath {
    COMPACTION_FRONTIER_MANIFEST_RELATIVE_PATH
        .split('/')
        .fold(prefix.clone(), ObjectPath::join)
}

#[derive(Clone, Debug)]
enum CompactionFrontierSource {
    Snapshot(CompactionFrontier),
    Shared(SharedCompactionFrontier),
}

impl CompactionFrontierSource {
    fn snapshot(&self) -> CompactionFrontier {
        match self {
            Self::Snapshot(frontier) => frontier.clone(),
            Self::Shared(frontier) => frontier.snapshot(),
        }
    }
}

struct ConfiguredObjectStore {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
}

type CompactionFrontierRefreshSource = (Arc<dyn ObjectStore>, ObjectPath);

#[cfg_attr(test, mutants::skip)]
fn build_configured_object_store(
    config: &ServiceConfig,
) -> Result<Option<ConfiguredObjectStore>, ServiceConfigError> {
    let Some(raw_url) = config.object_store_url.as_deref() else {
        return Ok(None);
    };

    match Url::parse(raw_url) {
        Ok(url) if url.scheme() == "file" => {
            let path =
                url.to_file_path()
                    .map_err(|()| ServiceConfigError::InvalidObjectStoreUrl {
                        url: raw_url.to_string(),
                        reason: "file URL must map to a local filesystem path".to_string(),
                    })?;
            Ok(Some(ConfiguredObjectStore {
                store: Arc::new(LocalFileSystem::new_with_prefix(path)?),
                prefix: ObjectPath::from(""),
            }))
        }
        Ok(url) => {
            let (store, prefix) = parse_url_opts(&url, std::env::vars())?;
            Ok(Some(ConfiguredObjectStore {
                store: Arc::from(store),
                prefix,
            }))
        }
        Err(url::ParseError::RelativeUrlWithoutBase) => Ok(Some(ConfiguredObjectStore {
            store: Arc::new(LocalFileSystem::new_with_prefix(raw_url)?),
            prefix: ObjectPath::from(""),
        })),
        Err(error) => Err(ServiceConfigError::InvalidObjectStoreUrl {
            url: raw_url.to_string(),
            reason: error.to_string(),
        }),
    }
}

async fn load_querier_shared_compaction_frontier(
    config: &ServiceConfig,
    configured_store: Option<&ConfiguredObjectStore>,
    object_store: Option<&dyn ObjectStore>,
) -> Result<
    (
        Option<SharedCompactionFrontier>,
        Option<CompactionFrontierRefreshSource>,
    ),
    ServiceConfigError,
> {
    if let Some(configured_store) = configured_store
        && let Some(prefix) = querier_object_store_prefix(config, Some(&configured_store.prefix))?
    {
        let frontier =
            shared_compaction_frontier_from_object_store(configured_store.store.as_ref(), &prefix)
                .await?;
        return Ok((
            Some(frontier),
            Some((configured_store.store.clone(), prefix)),
        ));
    }

    if let Some(store) = object_store
        && let Some(prefix) = querier_object_store_prefix(config, None)?
    {
        return Ok((
            Some(shared_compaction_frontier_from_object_store(store, &prefix).await?),
            None,
        ));
    }

    Ok((None, None))
}

fn compactor_delete_requests_for_config(
    config: &ServiceConfig,
    provided: Option<SharedLogDeleteRequests>,
) -> Result<SharedLogDeleteRequests, LogDeleteRequestStoreError> {
    match provided {
        Some(delete_requests) => Ok(delete_requests),
        None => SharedLogDeleteRequests::from_data_root(&config.data_root),
    }
}

