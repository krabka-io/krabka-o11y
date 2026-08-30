use super::{PartitionIndex, Offset};

/// Errors raised while adapting Kafka consumer records to compactor WAL records.
#[derive(Debug, thiserror::Error)]
pub enum CompactionConsumerRecordError {
    #[error("metrics WAL record at partition {partition} offset {offset} has no value")]
    MissingValue {
        partition: PartitionIndex,
        offset: Offset,
    },
}
