use super::{
    BlockDescriptor, ObjectStore, ServiceConfig, ServiceConfigError, ServiceDependencies,
    ServiceRuntimeError, TenantCompactionIndexCache, advance_and_persist_compaction_frontier,
    build_compactor_configured_object_store, compactor_delete_requests_for_config,
    compactor_object_store, effective_object_store_prefix, load_existing_compaction_frontier,
    materialize_delete_requests_in_existing_local_manifest_blocks,
    materialize_deletes_then_compact_next_kafka_wal_batch, validate_compactor_policy,
};

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
