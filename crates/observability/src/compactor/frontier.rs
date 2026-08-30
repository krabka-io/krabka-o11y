use object_store::ObjectStoreExt;

use crate::{
    ActiveLogDeleteFilter, ActiveLogDeleteFilterError, Arc, BTreeMap, BlockDescriptor, BlockIndex,
    BlockKey, BlockStoreError, CompactionCommitError, CompactionError, CompactionOffsetCommitter,
    Deserialize, Error, LabelIndex, Labels, LogCompactionIndexOutput, LogRow, LogWalConsumer,
    Mutex, ObjectPath, ObjectStore, Offset, PartitionIndex, Serialize, SeriesFingerprint, Time,
    TimeRange, WalConsumerError, WalRecordDecodeError,
    compact_log_block_to_object_store_with_index_output, decode_kafka_wal_record_envelope,
    is_deleted_log_entry,
};

// === split-modules: generated submodules ===
mod compact_kafka_wal_records_to_object_store;
mod compact_next_kafka_wal_batch_to_object_store;
mod compact_wal_records_to_object_store;
mod compact_wal_records_to_object_store_with_delete_filters_and_index_output;
mod compaction_frontier;
mod compaction_frontier_manifest;
mod compaction_frontier_manifest_object_path;
mod compaction_frontier_manifest_relative_path;
mod compaction_frontier_manifest_version;
mod compaction_frontier_refresh_source;
mod compaction_frontier_source;
mod compaction_frontier_store_error;
mod compactor_run_error;
mod configured_object_store;
mod kafka_wal_compaction_error;
mod kafka_wal_header;
mod kafka_wal_record;
mod last_compacted_position;
mod read_compaction_frontier_from_object_store;
mod shared_compaction_frontier;
mod wal_log_record;
mod wal_position;
mod write_compaction_frontier_to_object_store;

pub use compact_kafka_wal_records_to_object_store::compact_kafka_wal_records_to_object_store;
pub use compact_next_kafka_wal_batch_to_object_store::compact_next_kafka_wal_batch_to_object_store;
pub use compact_wal_records_to_object_store::compact_wal_records_to_object_store;
pub (crate) use compact_wal_records_to_object_store_with_delete_filters_and_index_output::compact_wal_records_to_object_store_with_delete_filters_and_index_output;
pub use compaction_frontier::CompactionFrontier;
pub (crate) use compaction_frontier_manifest::CompactionFrontierManifest;
pub (crate) use compaction_frontier_manifest_object_path::compaction_frontier_manifest_object_path;
pub (crate) use compaction_frontier_manifest_relative_path::COMPACTION_FRONTIER_MANIFEST_RELATIVE_PATH;
pub (crate) use compaction_frontier_manifest_version::COMPACTION_FRONTIER_MANIFEST_VERSION;
pub (crate) use compaction_frontier_refresh_source::CompactionFrontierRefreshSource;
pub (crate) use compaction_frontier_source::CompactionFrontierSource;
pub use compaction_frontier_store_error::CompactionFrontierStoreError;
pub use compactor_run_error::CompactorRunError;
pub (crate) use configured_object_store::ConfiguredObjectStore;
pub use kafka_wal_compaction_error::KafkaWalCompactionError;
pub use kafka_wal_header::KafkaWalHeader;
pub use kafka_wal_record::KafkaWalRecord;
pub (crate) use last_compacted_position::LastCompactedPosition;
pub use read_compaction_frontier_from_object_store::read_compaction_frontier_from_object_store;
pub use shared_compaction_frontier::SharedCompactionFrontier;
pub use wal_log_record::WalLogRecord;
pub use wal_position::WalPosition;
pub use write_compaction_frontier_to_object_store::write_compaction_frontier_to_object_store;
