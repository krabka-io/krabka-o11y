use super::{BlockDeletionReport, BlockMeta, OrphanSweepStats};

/// What one pass of [`run_lifecycle_pass`](super::run_lifecycle_pass) did.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LifecycleReport {
    /// The blocks a merge wrote.
    pub compacted: Vec<BlockMeta>,
    /// Blocks dropped for falling outside their tenant's retention window.
    pub expired: usize,
    /// The deletion of the retired merge inputs and the expired blocks,
    /// sidecars included.
    pub deletions: BlockDeletionReport,
    /// The orphan sweep over the block prefix.
    pub orphans: OrphanSweepStats,
}

impl LifecycleReport {
    /// Whether the pass changed the index.
    ///
    /// A pass that changed nothing publishes no snapshot. A save on every tick
    /// burns a snapshot generation and evicts the retained history a reader
    /// falls back on.
    #[must_use]
    pub fn changed_the_index(&self) -> bool {
        !self.compacted.is_empty() || self.expired > 0
    }
}
