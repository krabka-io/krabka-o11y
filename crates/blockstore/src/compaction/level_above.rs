use super::BlockLevel;

/// The level of a block that replaces blocks sitting at `sources`.
///
/// One rung above the highest of them, so a compaction of level-`L` blocks
/// produces a level-`L + 1` block and a [`super::CompactionPolicy`] can cap
/// how many times the same rows are rewritten.
///
/// A replacement with no sources replaced no compacted block either, so it
/// sits at [`BlockLevel::INGESTED`] rather than one rung above nothing.
#[must_use]
pub fn level_above(sources: impl IntoIterator<Item = BlockLevel>) -> BlockLevel {
    sources
        .into_iter()
        .max()
        .map_or(BlockLevel::INGESTED, BlockLevel::next)
}
