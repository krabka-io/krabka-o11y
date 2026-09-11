use super::EncodeLabelSet;

/// The `topic` and `partition` labels the WAL consumer families carry.
///
/// A role subscribes to one topic and is assigned a subset of its partitions,
/// so the label set is bounded by the partition count the topic contract
/// provisions.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct WalPartitionLabel {
    pub topic: String,
    pub partition: i32,
}
