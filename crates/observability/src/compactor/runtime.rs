use crate::WalPosition;
use crate::{
    Arc, BTreeMap, BlockDescriptor, BlockIndex, BlockStoreError, BufferedLogHotTail,
    CancellationToken, CompactionError, CompactionFrontierStoreError, CompactorRunError,
    KafkaWalCompactionError, KafkaWalRecord, LabelIndex, LastCompactedPosition,
    LogCompactionIndexOutput, LogWalConsumer, ObjectPath, ObjectStore, Offset, PartitionIndex,
    ServiceConfig, ServiceConfigError, ServiceDependencies, ServiceRuntimeError,
    SharedCompactionFrontier, SharedLogDeleteRequests, TenantCompactionIndexCache, Time, TimeExt,
    active_log_delete_filters_from_requests, active_log_delete_tenants,
    build_compactor_configured_object_store,
    compact_wal_records_to_object_store_with_delete_filters_and_index_output,
    compactor_delete_requests_for_config, compactor_object_store, decode_kafka_wal_record_envelope,
    effective_object_store_prefix, materialize_delete_requests_in_existing_local_manifest_blocks,
    materialize_delete_requests_in_existing_object_store_blocks,
    poll_accumulated_log_compaction_records, read_compaction_frontier_from_object_store, sleep,
    validate_compactor_policy, wal_compaction_chunks, wal_record_time_range,
    write_compaction_frontier_to_object_store,
};
use tracing::Instrument;
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
pub(crate) fn next_compactor_object_store_backoff(current: Time, max_backoff: Time) -> Time {
    (current * 2.0).min(max_backoff)
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

pub(crate) fn block_store_error_is_object_store(error: &BlockStoreError) -> bool {
    matches!(error, BlockStoreError::ObjectStore(_))
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

pub(crate) async fn shared_compaction_frontier_from_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> Result<SharedCompactionFrontier, CompactionFrontierStoreError> {
    let frontier = SharedCompactionFrontier::default();
    load_existing_compaction_frontier(store, prefix, &frontier).await?;
    Ok(frontier)
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

#[cfg_attr(test, mutants::skip)]
pub(crate) fn spawn_compaction_frontier_refresher(
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

pub(crate) async fn materialize_deletes_then_compact_next_kafka_wal_batch(
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

pub(crate) async fn compact_next_kafka_wal_batch_to_object_store_from_existing_manifest(
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

pub(crate) async fn materialize_log_deletes_before_compaction(
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
pub(crate) fn set_remote_parent_from_wal_records(span: &tracing::Span, records: &[KafkaWalRecord]) {
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

pub(crate) async fn compact_polled_kafka_wal_records_to_object_store_from_existing_manifest(
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
