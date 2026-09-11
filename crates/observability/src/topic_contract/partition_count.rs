use std::fmt;

use super::TopicContractError;

/// The number of partitions on a topic.
///
/// A partition count is not a capacity knob here. The producer routes a keyed
/// record with `murmur2(key) % partition_count`, so the count **is** the
/// write-path shard count: it decides which shard a series, a trace or an
/// entity belongs to. Two readers that disagree about it disagree about where
/// a key lives, and the order a consumer sees for one key stops being the
/// order the producer wrote.
///
/// There is no second place that configures a shard count. Provisioning sets
/// this number once, and every other component derives it from broker
/// metadata -- the producer through its own metadata refresh, and a caller
/// that needs the number through
/// [`shard_count`](super::shard_count).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PartitionCount(i32);

impl PartitionCount {
    /// Builds a partition count.
    ///
    /// # Errors
    /// Returns [`TopicContractError::InvalidPartitionCount`] when `count` is
    /// not positive. Zero shards accept no records, and Kafka reserves
    /// negative values for "use the broker default", which is the silent
    /// mis-provisioning this contract exists to stop.
    pub const fn new(count: i32) -> Result<Self, TopicContractError> {
        if count <= 0 {
            return Err(TopicContractError::InvalidPartitionCount { count });
        }
        Ok(Self(count))
    }

    /// The count as the `i32` the Kafka protocol carries.
    #[must_use]
    pub const fn get(self) -> i32 {
        self.0
    }
}

impl fmt::Display for PartitionCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
