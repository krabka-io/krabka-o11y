use super::{BTreeMap, Index};

/// One grid slot of a published profile index, in memory.
///
/// The block records, the series their postings reach and the label postings
/// themselves are an [`Index`] of a single tenant, which is the same structure
/// the metrics path shards and the same encoding. The stacktrace partitions
/// ride alongside because nothing else in the shared index knows about them,
/// and because they are per block: keeping them in one fleet-wide map is what
/// made a flush rewrite every block's partitions to publish one block's.
#[derive(Default)]
pub(crate) struct ProfileShard {
    pub(crate) index: Index,
    /// `object key -> stacktrace partitions`, for the blocks in this shard.
    pub(crate) partitions: BTreeMap<String, Vec<u64>>,
}
