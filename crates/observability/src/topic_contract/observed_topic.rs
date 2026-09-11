use std::collections::BTreeMap;

use super::PartitionCount;

/// What the broker reports about one topic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedTopic {
    /// The topic name.
    pub name: String,

    /// The live partition count, which is the live shard count.
    pub partitions: PartitionCount,

    /// Replicas on the topic's first partition.
    pub replication_factor: i32,

    /// The topic's explicit configuration overrides.
    ///
    /// Broker defaults are absent from this map. `DescribeConfigs` on this
    /// broker returns per-topic overrides only, so a key that nobody set is
    /// reported by its absence and not by its effective value.
    pub overrides: BTreeMap<String, String>,
}
