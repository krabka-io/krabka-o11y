use tracing::Instrument;

use crate::{
    Arc, BTreeMap, BlockDescriptor, BlockIndex, BlockStoreError, BufferedLogHotTail,
    CancellationToken, CompactionError, CompactionFrontierStoreError, CompactorRunError,
    KafkaWalCompactionError, KafkaWalRecord, LabelIndex, LastCompactedPosition,
    LogCompactionIndexOutput, LogWalConsumer, ObjectPath, ObjectStore, Offset, PartitionIndex,
    ServiceConfig, ServiceConfigError, ServiceDependencies, ServiceRuntimeError,
    SharedCompactionFrontier, SharedLogDeleteRequests, TenantCompactionIndexCache, Time, TimeExt,
    WalPosition, active_log_delete_filters_from_requests, active_log_delete_tenants,
    build_compactor_configured_object_store,
    compact_wal_records_to_object_store_with_delete_filters_and_index_output,
    compactor_delete_requests_for_config, compactor_object_store, decode_kafka_wal_record_envelope,
    effective_object_store_prefix, materialize_delete_requests_in_existing_local_manifest_blocks,
    materialize_delete_requests_in_existing_object_store_blocks,
    poll_accumulated_log_compaction_records, read_compaction_frontier_from_object_store, sleep,
    validate_compactor_policy, wal_compaction_chunks, wal_record_time_range,
    write_compaction_frontier_to_object_store,
};

// === split-modules: generated submodules ===
mod advance_and_persist_compaction_frontier;
mod block_store_error_is_object_store;
mod compact_next_kafka_wal_batch_to_object_store_from_existing_manifest;
mod compact_polled_kafka_wal_records_inner;
mod compact_polled_kafka_wal_records_to_object_store_from_existing_manifest;
mod compaction_error_is_object_store;
mod compactor_run_error_is_object_store;
mod load_existing_compaction_frontier;
mod materialize_deletes_then_compact_next_kafka_wal_batch;
mod materialize_log_deletes_before_compaction;
mod next_compactor_object_store_backoff;
mod refresh_compaction_frontier_and_prune;
mod run_compactor_until_shutdown;
mod set_remote_parent_from_wal_records;
mod shared_compaction_frontier_from_object_store;
mod spawn_compaction_frontier_refresher;

pub(crate) use advance_and_persist_compaction_frontier::advance_and_persist_compaction_frontier;
pub(crate) use block_store_error_is_object_store::block_store_error_is_object_store;
pub(crate) use compact_next_kafka_wal_batch_to_object_store_from_existing_manifest::compact_next_kafka_wal_batch_to_object_store_from_existing_manifest;
pub(crate) use compact_polled_kafka_wal_records_inner::compact_polled_kafka_wal_records_inner;
pub(crate) use compact_polled_kafka_wal_records_to_object_store_from_existing_manifest::compact_polled_kafka_wal_records_to_object_store_from_existing_manifest;
pub(crate) use compaction_error_is_object_store::compaction_error_is_object_store;
pub(crate) use compactor_run_error_is_object_store::compactor_run_error_is_object_store;
pub(crate) use load_existing_compaction_frontier::load_existing_compaction_frontier;
pub(crate) use materialize_deletes_then_compact_next_kafka_wal_batch::materialize_deletes_then_compact_next_kafka_wal_batch;
pub(crate) use materialize_log_deletes_before_compaction::materialize_log_deletes_before_compaction;
pub(crate) use next_compactor_object_store_backoff::next_compactor_object_store_backoff;
pub(crate) use refresh_compaction_frontier_and_prune::refresh_compaction_frontier_and_prune;
#[cfg_attr(test, mutants::skip)]
pub use run_compactor_until_shutdown::run_compactor_until_shutdown;
pub(crate) use set_remote_parent_from_wal_records::set_remote_parent_from_wal_records;
pub(crate) use shared_compaction_frontier_from_object_store::shared_compaction_frontier_from_object_store;
#[cfg_attr(test, mutants::skip)]
pub(crate) use spawn_compaction_frontier_refresher::spawn_compaction_frontier_refresher;
