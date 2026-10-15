//! HA election consumer records, errors, and protocols.

use super::*;

#[async_trait::async_trait]
pub trait HaElectionConsumerCommit: Send {
    async fn commit_sync(&mut self) -> Result<(), HaElectionConsumerError>;
}

#[derive(Debug, thiserror::Error)]
pub enum HaElectionConsumerError {
    #[error("HA election consumer poll failed: {0}")]
    Poll(String),

    #[error("HA election record has unsupported format: {0}")]
    UnsupportedFormat(String),

    #[error(transparent)]
    Replay(#[from] HaElectionReplayError),

    #[error("HA election consumer commit failed: {0}")]
    Commit(String),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HaElectionConsumerLoopSummary {
    pub polls: usize,
    pub polled_records: usize,
    pub replayed_records: usize,
    pub committed_offsets: Vec<HaElectionPartitionOffset>,
}

impl HaElectionConsumerLoopSummary {
    pub(crate) fn record_poll(
        &mut self,
        polled_records: usize,
        replayed_records: usize,
        committed_offsets: impl IntoIterator<Item = HaElectionPartitionOffset>,
    ) {
        self.polls += 1;
        self.polled_records += polled_records;
        self.replayed_records += replayed_records;
        self.committed_offsets.extend(committed_offsets);
    }
}

#[async_trait::async_trait]
pub trait HaElectionConsumerPoll: Send {
    async fn poll(&mut self, timeout: Time)
    -> Result<Vec<ConsumerRecord>, HaElectionConsumerError>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HaElectionConsumerRecord {
    pub topic: String,
    pub partition: PartitionIndex,
    pub offset: Offset,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HaElectionPartitionOffset {
    pub partition: PartitionIndex,
    /// Kafka commit offset. This is the next offset after the last replayed
    /// record.
    pub offset: Offset,
}

#[derive(Debug, thiserror::Error)]
pub enum HaElectionReplayError {
    #[error("HA election record at partition {partition} offset {offset} has no value")]
    MissingValue {
        partition: PartitionIndex,
        offset: Offset,
    },

    #[error("HA election record decode failed: {0}")]
    Decode(String),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HaElectionReplayResult {
    pub polled_records: usize,
    pub replayed_records: usize,
    pub committed_offsets: Vec<HaElectionPartitionOffset>,
}

/// Testable sink for compacted HA election records.
#[async_trait::async_trait]
pub trait HaElectionSink: Send + Sync {
    async fn persist_election(&self, record: HaElectionRecord) -> Result<(), ProduceError>;
}
