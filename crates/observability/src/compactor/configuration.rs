use krabka_client_core::ClientSecurity;
use krabka_units::convert::{StdDurationExt, TimeExt};

use crate::{
    BlockDescriptor, BrokerAccessPolicy, BrokerBackedIngestLimiter, ClientResourcePolicy,
    ConfiguredObjectStore, DeferredQueryAuthorizerConnect, DeferredWalConsumerConnect,
    KafkaLogWalConsumer, KafkaLogWalSink, ObjectPath, ObjectStore, Role, ServiceConfig,
    ServiceConfigError, ServiceDependencies, ServiceRuntimeError, Time,
    advance_and_persist_compaction_frontier, build_configured_object_store,
    effective_object_store_prefix, materialize_deletes_then_compact_next_kafka_wal_batch, sleep,
    wal_consumer_metrics::WalConsumerMetrics,
};
mod all_in_one_querier_group_id;
mod build_compactor_configured_object_store;
mod build_service_dependencies;
mod build_service_dependencies_with_client_resource_policy;
mod compactor_object_store;
mod connect_with_startup_retry;
mod run_compactor_once;
mod run_compactor_until_idle;
mod validate_compactor_policy;
mod validate_distributor_policy;
mod with_block_builder_dependencies;
mod with_distributor_dependencies;
mod with_querier_dependencies;

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

pub(crate) use all_in_one_querier_group_id::all_in_one_querier_group_id;
pub(crate) use build_compactor_configured_object_store::build_compactor_configured_object_store;
pub use build_service_dependencies::build_service_dependencies;
pub use build_service_dependencies_with_client_resource_policy::build_service_dependencies_with_client_resource_policy;
pub(crate) use compactor_object_store::compactor_object_store;
#[cfg_attr(test, mutants::skip)]
pub(crate) use connect_with_startup_retry::connect_with_startup_retry;
pub use run_compactor_once::run_compactor_once;
pub use run_compactor_until_idle::run_compactor_until_idle;
use run_compactor_until_idle::{CompactorBatches, CompactorRun};
pub(crate) use validate_compactor_policy::validate_compactor_policy;
pub(crate) use validate_distributor_policy::validate_distributor_policy;
pub(crate) use with_block_builder_dependencies::with_block_builder_dependencies;
pub(crate) use with_distributor_dependencies::with_distributor_dependencies;
pub(crate) use with_querier_dependencies::with_querier_dependencies;
