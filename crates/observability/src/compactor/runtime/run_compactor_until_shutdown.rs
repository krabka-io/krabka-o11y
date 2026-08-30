use super::{
    BlockDescriptor, CompactorRunError, KafkaWalRecord, ObjectStore, ServiceConfig,
    ServiceConfigError, ServiceDependencies, ServiceRuntimeError, TenantCompactionIndexCache, Time,
    TimeExt, advance_and_persist_compaction_frontier, build_compactor_configured_object_store,
    compact_polled_kafka_wal_records_to_object_store_from_existing_manifest,
    compactor_delete_requests_for_config, compactor_object_store,
    compactor_run_error_is_object_store, effective_object_store_prefix,
    load_existing_compaction_frontier,
    materialize_delete_requests_in_existing_local_manifest_blocks,
    materialize_log_deletes_before_compaction, next_compactor_object_store_backoff,
    poll_accumulated_log_compaction_records, sleep, validate_compactor_policy,
};

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
