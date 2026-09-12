use super::{BlockDeletionFailure, CompactionRetentionPhase, OrphanSweepStats};

/// What one pass of
/// [`enforce_compaction_retention`](super::enforce_compaction_retention) did.
///
/// The pass has three ordered parts and each keeps its own counts. It retires
/// the index manifests of the blocks that fell outside their tenant's window,
/// then deletes the block objects those manifests named, then deletes the
/// objects no manifest names at all.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompactionRetentionStats {
    /// Index manifests read under the metrics prefix. Every live block has
    /// one, so this is the block count the pass judged.
    pub manifests_scanned: usize,
    /// What the index retirement did: the `.index` manifests of the expired
    /// blocks, deleted before the blocks themselves.
    pub manifests_retired: CompactionRetentionPhase,
    /// What the block deletion did. A block whose manifest would not delete is
    /// not counted here, because the pass leaves it alone.
    pub blocks_deleted: CompactionRetentionPhase,
    /// What the orphan reconciliation did.
    pub orphans: OrphanSweepStats,
}

impl CompactionRetentionStats {
    /// Every object the pass could not delete, in the order it tried them.
    ///
    /// A caller logs one line per failure. Totals alone would hide an object
    /// that fails on every pass, and that object is the one an operator has to
    /// act on.
    pub fn failures(&self) -> impl Iterator<Item = &BlockDeletionFailure> {
        self.manifests_retired
            .failures
            .iter()
            .chain(self.blocks_deleted.failures.iter())
    }

    /// Whether the pass deleted any object at all.
    #[must_use]
    pub const fn deleted_anything(&self) -> bool {
        self.manifests_retired.deleted > 0
            || self.blocks_deleted.deleted > 0
            || self.orphans.deleted > 0
    }
}
