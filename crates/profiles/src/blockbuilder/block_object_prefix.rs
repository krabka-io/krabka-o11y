/// The object-store prefix every profile block sits under.
///
/// Both writers of a block key start here: [`object_key`](super::object_key)
/// for a block the block-builder flushes, and the compactor for a block a
/// merge writes. The orphan sweep lists this prefix and deletes what the index
/// does not name, so a block written outside it would never be reclaimed, and
/// anything else written inside it would be deleted.
pub const BLOCK_OBJECT_PREFIX: &str = "blocks";
