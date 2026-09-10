use super::IndexShardRange;

/// What an object under the index-shard prefix turned out to be.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexShardObject {
    /// A shard covering the given span.
    Shard(IndexShardRange),
    /// The tenant's series that no block carries yet.
    UnboundSeries,
    /// Not written by this module. Neither read nor deleted.
    Foreign,
}
