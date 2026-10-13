use krabka_blockstore::ObjectStoreMetrics;

use super::{
    super::prepare_compactor_wal_inputs, BlockDescriptor, CompactorRunError, Instant,
    KafkaWalRecord, ObjectStore, ServiceConfig, ServiceDependencies, ServiceRuntimeError, Time,
    TimeExt, advance_and_persist_compaction_frontier, build_compactor_configured_object_store,
    compact_polled_kafka_wal_records_to_object_store_from_existing_manifest,
    compactor_object_store, compactor_run_error_is_object_store, limits_provider_for_config,
    materialize_log_deletes_before_compaction, next_compactor_object_store_backoff,
    poll_accumulated_log_compaction_records, sleep, sweep_log_retention_before_compaction,
    validate_compactor_policy,
};
use crate::compaction_metrics::CompactionMetrics;

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
    let started = wal_inputs.start().await;
    let compaction_frontier = started.compaction_frontier;
    let delete_requests = started.delete_requests;
    let run = started.run;
    let super::super::CompactorRunState {
        mut consumer,
        mut descriptors,
        mut tenant_indexes,
    } = run;
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
