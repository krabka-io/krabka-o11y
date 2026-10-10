use super::{
    CompactionMetrics, ObjectStoreMetrics, Registry, WalConsumerMetrics, WalProduceMetrics,
};

/// The WAL-consumer, WAL-produce, compaction and object-store bundles.
///
/// Every signal registers these under its own prefix, so the signals export
/// the same instruments and one dashboard reads all of them.
pub struct PipelineInstruments {
    /// WAL consumer progress and receive delay.
    pub wal_consumer: WalConsumerMetrics,
    /// Partial WAL batches and the records they left unacked.
    pub wal_produce: WalProduceMetrics,
    /// Compaction passes, their outcome and their output.
    pub compaction: CompactionMetrics,
    /// Object-store requests, latencies, failures and retries.
    pub object_store: ObjectStoreMetrics,
}

impl PipelineInstruments {
    /// Registers the four bundles in `registry`, in the order WAL consumer,
    /// WAL produce, compaction, object store.
    pub fn register(registry: &mut Registry) -> Self {
        Self {
            wal_consumer: WalConsumerMetrics::register(registry),
            wal_produce: WalProduceMetrics::register(registry),
            compaction: CompactionMetrics::register(registry),
            object_store: ObjectStoreMetrics::register(registry),
        }
    }
}
