use krabka_units::{Time, convert::TimeExt as _, minutes};

use super::{PartitionCount, TopicContractError};

/// The deployment-specific half of the topic contract.
///
/// The fields are public and the struct is built with a literal rather than a
/// constructor on purpose: `wal_partitions` and `state_partitions` are the
/// same type, and a positional constructor would let a caller transpose them
/// without the compiler noticing.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TopicSettings {
    /// Partitions on each of the four WAL topics, which is the write-path
    /// shard count for that signal. Raising it later re-maps every key, so it
    /// is set once at provisioning and checked on every start.
    pub wal_partitions: PartitionCount,

    /// Partitions on the two compacted state topics. Compaction keeps the
    /// last record per key **per partition**, so this count shards the state
    /// map the same way and carries the same one-way hazard.
    pub state_partitions: PartitionCount,

    /// Replicas per partition. One is right for a single-broker development
    /// cluster and wrong for anything that must survive a broker loss.
    pub replication_factor: i32,

    /// `retention.ms` on the WAL topics. It bounds how far the block-builder
    /// may fall behind before the broker starts dropping records it has not
    /// read.
    pub wal_retention: Time,
}

impl TopicSettings {
    /// The single-broker development default: one shard per signal, one
    /// replica, and a fifteen-minute WAL window.
    ///
    /// # Panics
    /// Never. The literal partition counts are positive.
    #[must_use]
    pub fn single_broker() -> Self {
        let one = PartitionCount::new(1).expect("1 is a positive partition count");
        Self {
            wal_partitions: one,
            state_partitions: one,
            replication_factor: 1,
            wal_retention: minutes(15),
        }
    }

    /// Rejects settings the broker would take but the stack cannot use.
    ///
    /// # Errors
    /// Returns [`TopicContractError::InvalidReplicationFactor`] when the
    /// replication factor is not positive, and
    /// [`TopicContractError::InvalidRetention`] when the WAL retention is not
    /// positive. A zero or negative WAL window means the broker may drop a
    /// record the moment it is written.
    pub fn validate(&self) -> Result<(), TopicContractError> {
        if self.replication_factor <= 0 {
            return Err(TopicContractError::InvalidReplicationFactor {
                factor: self.replication_factor,
            });
        }
        if self.wal_retention <= Time::ZERO {
            return Err(TopicContractError::InvalidRetention {
                retention: self.wal_retention,
            });
        }
        Ok(())
    }
}

impl Default for TopicSettings {
    fn default() -> Self {
        Self::single_broker()
    }
}
