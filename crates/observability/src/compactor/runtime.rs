use tracing::Instrument;
use krabka_blockstore::ObjectStoreMetrics;

use crate::compactor::{prepare_compactor_wal_inputs, CompactorRunComponents};
use crate::{
    Arc, BTreeMap, BlockDescriptor, BlockStoreError, BufferedLogHotTail, CancellationToken,
    CompactionError, CompactionFrontierStoreError, CompactorRunError, Instant, JoinHandle,
    KafkaWalCompactionError, KafkaWalHeader, KafkaWalRecord, LastCompactedPosition,
    LogCompactionIndexOutput, LogWalConsumer, ObjectPath, ObjectStore, Offset, OverridesProvider,
    PartitionIndex, ServiceConfig, ServiceDependencies, ServiceRuntimeError,
    SharedCompactionFrontier, SharedLogDeleteRequests, SystemTime, TenantCompactionIndexCache,
    Time, TimeExt, UNIX_EPOCH, WalPosition, active_log_delete_filters_from_requests,
    active_log_delete_tenants, build_compactor_configured_object_store,
    compact_wal_records_to_object_store_with_delete_filters_and_index_output,
    compaction_metrics::CompactionMetrics, compactor::frontier::CompactionWriteOptions,
    compactor_object_store, decode_kafka_wal_record_envelope, limits_provider_for_config,
    materialize_delete_requests_in_existing_object_store_blocks,
    poll_accumulated_log_compaction_records, read_compaction_frontier_from_object_store, sleep,
    sweep_expired_log_blocks, validate_compactor_policy, wal_compaction_chunks,
    wal_record_time_range, write_compaction_frontier_to_object_store,
};

pub(crate) struct NextKafkaBatch<'a> {
    pub store: &'a dyn ObjectStore,
    pub prefix: &'a ObjectPath,
    pub consumer: &'a mut dyn LogWalConsumer,
    pub poll_timeout: Time,
    pub delete_requests: &'a SharedLogDeleteRequests,
    pub tenant_indexes: &'a mut TenantCompactionIndexCache,
    pub metrics: &'a CompactionMetrics,
}

pub(crate) fn block_store_error_is_object_store(error: &BlockStoreError) -> bool {
    matches!(error, BlockStoreError::ObjectStore(_))
}

/// Doubles the object-store retry backoff, up to a cap.
///
/// `Time` is `PartialOrd` but not `Ord`, so this uses `Time::min` and not
/// `std::cmp::min`.
pub(crate) fn next_compactor_object_store_backoff(current: Time, max_backoff: Time) -> Time {
    (current * 2.0).min(max_backoff)
}

pub(crate) async fn advance_and_persist_compaction_frontier(
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

pub(crate) async fn compact_next_kafka_wal_batch_to_object_store_from_existing_manifest(
    batch: NextKafkaBatch<'_>,
) -> Result<Vec<BlockDescriptor>, CompactorRunError> {
    let NextKafkaBatch {
        store,
        prefix,
        consumer,
        poll_timeout,
        delete_requests,
        tenant_indexes,
        metrics,
    } = batch;
    let records = consumer.poll(poll_timeout).await?;
    compact_polled_kafka_wal_records_to_object_store_from_existing_manifest(
        store,
        prefix,
        consumer,
        records,
        delete_requests,
        tenant_indexes,
        metrics,
    )
    .await
}

pub(crate) fn compaction_error_is_object_store(error: &CompactionError) -> bool {
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

pub(crate) fn compactor_run_error_is_object_store(error: &CompactorRunError) -> bool {
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

pub(crate) async fn load_existing_compaction_frontier(
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

pub(crate) async fn refresh_compaction_frontier_and_prune(
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

pub(crate) async fn shared_compaction_frontier_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> Result<SharedCompactionFrontier, CompactionFrontierStoreError> {
    let frontier = SharedCompactionFrontier::default();
    load_existing_compaction_frontier(store, prefix, &frontier).await?;
    Ok(frontier)
}

pub(crate) async fn compact_polled_kafka_wal_records_inner(
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
        let indexes = tenant_indexes.entry(tenant.clone()).or_default();
        let mut committer = LastCompactedPosition::default();
        let time_range = wal_record_time_range(&chunk)?;
        let delete_filters =
            active_log_delete_filters_from_requests(delete_requests, &tenant, time_range)?;
        let descriptor = compact_wal_records_to_object_store_with_delete_filters_and_index_output(
            store,
            prefix,
            &mut indexes.label_index,
            &mut indexes.block_index,
            &mut committer,
            chunk,
            CompactionWriteOptions {
                delete_filters: &delete_filters,
                index_output: LogCompactionIndexOutput::ShardManifests,
            },
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

    for position in commit_positions {
        consumer
            .commit_compacted(WalPosition {
                partition: position.0,
                offset: position.1,
            })
            .await?;
    }

    Ok(descriptors)
}

/// Compacts one polled WAL batch and records the pass in `metrics`.
///
/// This is where every compactor entry point meets, so it is the one place the
/// compaction instruments have to move. A batch with no records returns before
/// the timer starts: nothing was compacted, and counting it as a pass would
/// make an idle compactor look busy. The poll that returned nothing is already
/// counted by the WAL consumer instruments.
pub(crate) async fn compact_polled_kafka_wal_records_to_object_store_from_existing_manifest(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    consumer: &mut (impl LogWalConsumer + ?Sized),
    records: Vec<KafkaWalRecord>,
    delete_requests: &SharedLogDeleteRequests,
    tenant_indexes: &mut TenantCompactionIndexCache,
    metrics: &CompactionMetrics,
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

    let started = Instant::now();
    let compacted = compact_polled_kafka_wal_records_inner(
        store,
        prefix,
        consumer,
        records,
        delete_requests,
        tenant_indexes,
    )
    .instrument(span)
    .await;
    metrics.record_run(compacted.is_ok(), Time::from_std(started.elapsed()));
    if let Ok(descriptors) = &compacted {
        metrics.record_output(descriptors.len() as u64);
    }
    compacted
}

pub(crate) async fn materialize_deletes_then_compact_next_kafka_wal_batch(
    batch: NextKafkaBatch<'_>,
) -> Result<Vec<BlockDescriptor>, CompactorRunError> {
    let NextKafkaBatch {
        store,
        prefix,
        consumer,
        poll_timeout,
        delete_requests,
        tenant_indexes,
        metrics,
    } = batch;
    materialize_log_deletes_before_compaction(
        store,
        prefix,
        delete_requests,
        tenant_indexes,
        metrics,
    )
    .await?;
    compact_next_kafka_wal_batch_to_object_store_from_existing_manifest(NextKafkaBatch {
        store,
        prefix,
        consumer,
        poll_timeout,
        delete_requests,
        tenant_indexes,
        metrics,
    })
    .await
}

pub(crate) async fn materialize_log_deletes_before_compaction(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    delete_requests: &SharedLogDeleteRequests,
    tenant_indexes: &mut TenantCompactionIndexCache,
    metrics: &CompactionMetrics,
) -> Result<(), CompactorRunError> {
    if !active_log_delete_tenants(delete_requests)?.is_empty() {
        materialize_delete_requests_in_existing_object_store_blocks(
            store,
            prefix,
            delete_requests,
            metrics,
        )
        .await?;
        tenant_indexes.clear();
    }
    Ok(())
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
#[cfg_attr(test, mutants::skip)]
pub async fn run_compactor_until_shutdown(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
    shutdown: impl Future<Output = ()>,
) -> Result<Vec<BlockDescriptor>, ServiceRuntimeError> {
    validate_compactor_policy(config)?;
    // Shared RED-metrics bundle for the `:9404` exporter. It is `None` in tests
    // that do not wire metrics, and an unregistered bundle records nothing,
    // which is the right reading for a compactor with no registry behind it.
    let metrics = dependencies.metrics.clone();
    let object_store_metrics = metrics
        .as_ref()
        .map_or_else(ObjectStoreMetrics::unregistered, |metrics| {
            metrics.object_store.clone()
        });
    let compaction_metrics = metrics
        .as_ref()
        .map_or_else(CompactionMetrics::unregistered, |metrics| {
            metrics.compaction.clone()
        });
    let configured_store =
        build_compactor_configured_object_store(config, object_store, object_store_metrics).await?;
    let object_store = compactor_object_store(config, object_store, configured_store.as_ref())?;
    let store = object_store.store;
    let prefix = object_store.prefix;
    // The retention window is a per-tenant limit, so the compactor reads the
    // same provider the distributor and the querier read. A role that was
    // handed one uses it; one that was not loads it from its own config, so a
    // compactor never runs on a window nobody configured.
    let overrides = match dependencies.limits.clone() {
        Some(overrides) => overrides,
        None => limits_provider_for_config(config)?,
    };
    let wal_inputs = prepare_compactor_wal_inputs(config, &dependencies, store, &prefix).await?;
    let CompactorRunComponents {
        compaction_frontier,
        delete_requests,
        mut consumer,
        mut descriptors,
        mut tenant_indexes,
    } = wal_inputs.start().await.into_components();
    let mut object_store_retry_backoff = config.compactor_object_store_initial_backoff;
    // Now, so the first pass of the loop sweeps rather than waiting an
    // interval out before it deletes anything.
    let mut next_retention_sweep = Instant::now();
    let mut pending_compaction_records: Option<Vec<KafkaWalRecord>> = None;
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            biased;
            () = &mut shutdown => return Ok(descriptors),
            () = sleep(<Time as TimeExt>::ZERO.to_std()) => {}
        }

        let prepared = match materialize_log_deletes_before_compaction(
            store,
            &prefix,
            &delete_requests,
            &mut tenant_indexes,
            &compaction_metrics,
        )
        .await
        {
            Ok(()) => {
                sweep_log_retention_before_compaction(
                    store,
                    &prefix,
                    overrides.as_ref(),
                    &mut tenant_indexes,
                    &mut next_retention_sweep,
                    config.compactor_retention_sweep_interval,
                    &compaction_metrics,
                )
                .await
            }
            Err(error) => Err(error),
        };
        let batch_result = match prepared {
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
                    let compacted =
                        compact_polled_kafka_wal_records_to_object_store_from_existing_manifest(
                            store,
                            &prefix,
                            consumer.as_mut(),
                            records,
                            &delete_requests,
                            &mut tenant_indexes,
                            &compaction_metrics,
                        )
                        .await;
                    match compacted {
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

/// Re-parents `span` from Kafka headers carrying a trace context.
///
/// The Kafka client exposes headers through its own representation. Callers
/// can first convert those external headers into [`KafkaWalHeader`] records,
/// keeping the conversion to the propagation API at this boundary.
pub fn set_remote_parent_from_kafka_headers(span: &tracing::Span, headers: &[KafkaWalHeader]) {
    if !headers.iter().any(|header| header.key == "traceparent") {
        return;
    }
    krabka_telemetry::propagation::set_remote_parent(
        span,
        headers.iter().map(|header| {
            (
                header.key.as_str(),
                header.value.as_deref().unwrap_or(&[][..]),
            )
        }),
    );
}

/// Re-parents `span` into the trace carried by the first record whose headers
/// include a `traceparent`.
///
/// There is one span per poll batch rather than one per record, so the first
/// record carrying a trace context stands for the batch. A record without one
/// is skipped rather than used: extracting from its headers would find no
/// context and leave the batch in a trace of its own.
pub(crate) fn set_remote_parent_from_wal_records(span: &tracing::Span, records: &[KafkaWalRecord]) {
    let Some(parent) = records
        .iter()
        .find(|rec| rec.headers.iter().any(|h| h.key == "traceparent"))
    else {
        return;
    };
    set_remote_parent_from_kafka_headers(span, &parent.headers);
}

/// Starts the querier's compaction-frontier refresher and hands its
/// [`JoinHandle`] to the caller.
///
/// The handle is the whole return value, and the caller must keep it. The task
/// is the only thing that moves the querier's frontier forward and prunes what
/// the compactor has taken over, so if it stops, the querier keeps answering
/// from the frontier it last read, and from hot-tail records already published
/// as blocks. Nothing about the answers says they are stale. A dropped handle
/// makes that outcome unobservable: a panic inside the loop ends the task and
/// leaves nobody to notice.
///
/// Push the handle onto the caller's background-task list, under a name, so
/// the service loop that supervises those tasks treats its exit the way it
/// treats the hot-tail poller's — cancel the shutdown token and fail the
/// process, rather than serve stale reads.
///
/// A refresh that merely fails is not an exit: the loop logs it, keeps the
/// last good frontier, and tries again on the next tick. Only cancellation
/// (a clean return) and a panic end the task.
/// Dropping the handle is the defect this signature exists to prevent, so it is
/// a compile error rather than a review catch.
#[must_use]
#[cfg_attr(test, mutants::skip)]
pub(crate) fn spawn_compaction_frontier_refresher(
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    frontier: SharedCompactionFrontier,
    hot_tail: BufferedLogHotTail,
    token: CancellationToken,
    refresh_interval: Time,
) -> JoinHandle<()> {
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
    })
}

/// Runs the retention sweep when one is due, and drops the index cache after
/// it.
///
/// The sweep runs here rather than in a task of its own. Every log index write
/// is a whole-object put with last-writer-wins semantics, and this loop holds
/// the tenant indexes in memory between batches. A sweeper in another process
/// would rewrite a manifest from a snapshot taken before this loop's last put,
/// and the block that put had just published would be gone from the index.
///
/// A deployment where no tenant has a window pays one call to
/// [`OverridesProvider::expires_any_blocks`] per loop and nothing else. The
/// interval bounds what the rest pay: a sweep reads every index of every
/// tenant, and this loop turns over far faster than a retention window moves.
///
/// # Errors
/// Returns the block-store error when the store cannot be listed, or when an
/// index cannot be read or written.
pub(crate) async fn sweep_log_retention_before_compaction(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    overrides: &OverridesProvider,
    tenant_indexes: &mut TenantCompactionIndexCache,
    next_sweep: &mut Instant,
    sweep_interval: Time,
    metrics: &CompactionMetrics,
) -> Result<(), CompactorRunError> {
    if !overrides.expires_any_blocks() {
        return Ok(());
    }
    let started = Instant::now();
    if started < *next_sweep {
        return Ok(());
    }
    *next_sweep = started + sweep_interval.to_std();

    // A clock before the epoch, or past the nanosecond range of an `i64`,
    // expires nothing rather than everything.
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|since| i64::try_from(since.as_nanos()).ok())
        .unwrap_or(i64::MIN);
    sweep_expired_log_blocks(store, prefix, now_ns, overrides, metrics).await?;
    // The sweep rewrote manifests that the cache may hold, so the cache goes
    // with them.
    tenant_indexes.clear();
    Ok(())
}
