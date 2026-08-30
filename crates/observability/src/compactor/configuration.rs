use krabka_units::convert::{StdDurationExt, TimeExt};

use crate::{
    BlockDescriptor, BrokerBackedIngestLimiter, ClientResourcePolicy, ConfiguredObjectStore,
    KafkaLogWalConsumer, KafkaLogWalSink, ObjectPath, ObjectStore, Role, ServiceConfig,
    ServiceConfigError, ServiceDependencies, ServiceRuntimeError, TenantCompactionIndexCache, Time,
    advance_and_persist_compaction_frontier, build_configured_object_store,
    compactor_delete_requests_for_config, effective_object_store_prefix,
    load_existing_compaction_frontier,
    materialize_delete_requests_in_existing_local_manifest_blocks,
    materialize_deletes_then_compact_next_kafka_wal_batch, sleep,
};

// === split-modules: generated submodules ===
mod build_compactor_configured_object_store;
mod build_service_dependencies;
mod build_service_dependencies_with_client_resource_policy;
mod compactor_object_store;
mod connect_with_startup_retry;
mod run_compactor_once;
mod run_compactor_until_idle;
mod validate_compactor_policy;
mod validate_distributor_policy;

pub(crate) use build_compactor_configured_object_store::build_compactor_configured_object_store;
pub use build_service_dependencies::build_service_dependencies;
pub use build_service_dependencies_with_client_resource_policy::build_service_dependencies_with_client_resource_policy;
pub(crate) use compactor_object_store::compactor_object_store;
#[cfg_attr(test, mutants::skip)]
pub(crate) use connect_with_startup_retry::connect_with_startup_retry;
pub use run_compactor_once::run_compactor_once;
pub use run_compactor_until_idle::run_compactor_until_idle;
pub(crate) use validate_compactor_policy::validate_compactor_policy;
pub(crate) use validate_distributor_policy::validate_distributor_policy;
