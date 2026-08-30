use super::{BlockWriter, CompactionLoopConfig, ObjectStoreCompactionIndexSink};

/// Runtime handles assembled for the compactor role.
pub struct MetricsCompactorRuntime {
    pub block_writer: BlockWriter,
    pub index_sink: ObjectStoreCompactionIndexSink,
    pub loop_config: CompactionLoopConfig,
}
