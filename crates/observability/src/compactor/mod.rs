pub(crate) mod configuration;
pub use configuration::{
    build_service_dependencies, build_service_dependencies_with_client_resource_policy,
    run_compactor_once, run_compactor_until_idle,
};
pub(crate) mod runtime;
pub use runtime::{run_compactor_until_shutdown, set_remote_parent_from_kafka_headers};

use crate::{
    ObjectPath, ObjectStore, ServiceConfig, ServiceConfigError, ServiceDependencies,
    ServiceRuntimeError, SharedLogDeleteRequests, compactor_delete_requests_for_config,
    load_existing_compaction_frontier,
    materialize_delete_requests_in_existing_local_manifest_blocks,
};

pub(crate) struct CompactorWalInputs {
    pub(crate) compaction_frontier: SharedCompactionFrontier,
    pub(crate) delete_requests: SharedLogDeleteRequests,
    pub(crate) consumer: std::sync::Arc<tokio::sync::Mutex<Box<dyn crate::LogWalConsumer>>>,
}

impl CompactorWalInputs {
    pub(crate) async fn start(&self) -> StartedCompactorRun<'_> {
        StartedCompactorRun {
            compaction_frontier: self.compaction_frontier.clone(),
            delete_requests: self.delete_requests.clone(),
            run: start_compactor_run(&self.consumer).await,
        }
    }
}

pub(crate) struct StartedCompactorRun<'a> {
    pub(crate) compaction_frontier: SharedCompactionFrontier,
    pub(crate) delete_requests: SharedLogDeleteRequests,
    pub(crate) run: CompactorRunState<'a>,
}

pub(crate) struct CompactorRunComponents<'a> {
    pub(crate) compaction_frontier: SharedCompactionFrontier,
    pub(crate) delete_requests: SharedLogDeleteRequests,
    pub(crate) consumer: tokio::sync::MutexGuard<'a, Box<dyn crate::LogWalConsumer>>,
    pub(crate) descriptors: Vec<crate::BlockDescriptor>,
    pub(crate) tenant_indexes: crate::TenantCompactionIndexCache,
}

impl<'a> StartedCompactorRun<'a> {
    pub(crate) fn into_components(self) -> CompactorRunComponents<'a> {
        let Self {
            compaction_frontier,
            delete_requests,
            run,
        } = self;
        CompactorRunComponents {
            compaction_frontier,
            delete_requests,
            consumer: run.consumer,
            descriptors: run.descriptors,
            tenant_indexes: run.tenant_indexes,
        }
    }
}

pub(crate) struct CompactorRunState<'a> {
    pub(crate) consumer: tokio::sync::MutexGuard<'a, Box<dyn crate::LogWalConsumer>>,
    pub(crate) descriptors: Vec<crate::BlockDescriptor>,
    pub(crate) tenant_indexes: crate::TenantCompactionIndexCache,
}

pub(crate) async fn start_compactor_run(
    consumer: &std::sync::Arc<tokio::sync::Mutex<Box<dyn crate::LogWalConsumer>>>,
) -> CompactorRunState<'_> {
    CompactorRunState {
        consumer: consumer.lock().await,
        descriptors: Vec::new(),
        tenant_indexes: crate::TenantCompactionIndexCache::new(),
    }
}

pub(crate) async fn prepare_compactor_wal_inputs(
    config: &ServiceConfig,
    dependencies: &ServiceDependencies,
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> Result<CompactorWalInputs, ServiceRuntimeError> {
    let compaction_frontier = dependencies.compaction_frontier.clone().unwrap_or_default();
    let delete_requests =
        compactor_delete_requests_for_config(config, dependencies.delete_requests.clone())?;
    load_existing_compaction_frontier(store, prefix, &compaction_frontier).await?;
    materialize_delete_requests_in_existing_local_manifest_blocks(
        &config.data_root,
        &delete_requests,
    )?;
    let consumer = dependencies
        .wal_consumer
        .clone()
        .ok_or(ServiceConfigError::MissingWalConsumer)?;
    Ok(CompactorWalInputs {
        compaction_frontier,
        delete_requests,
        consumer,
    })
}

pub(crate) mod delete_materialization;
pub use delete_materialization::{
    CompactionCommitError, CompactionError, CompactionOffsetCommitter,
    compact_log_block_to_object_store,
};
pub(crate) mod frontier;
pub use frontier::{
    CompactionFrontier, CompactionFrontierStoreError, CompactorRunError, KafkaWalCompactionError,
    KafkaWalHeader, KafkaWalRecord, SharedCompactionFrontier, WalLogRecord, WalPosition,
    compact_kafka_wal_records_to_object_store, compact_next_kafka_wal_batch_to_object_store,
    compact_wal_records_to_object_store, read_compaction_frontier_from_object_store,
    write_compaction_frontier_to_object_store,
};
pub(crate) mod object_store_support;
pub(crate) mod retention;
