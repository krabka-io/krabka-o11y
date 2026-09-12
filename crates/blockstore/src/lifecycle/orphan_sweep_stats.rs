/// What one pass of [`reconcile_orphans`](super::reconcile_orphans) did.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OrphanSweepStats {
    /// Objects found under the prefix.
    pub listed: usize,
    /// Objects the index named, and so left alone.
    pub live: usize,
    /// Objects the index did not name, but which are too new to judge. A
    /// writer that has put a block and not yet published it looks exactly like
    /// an orphan, so these are kept.
    pub kept_within_grace: usize,
    /// Objects deleted.
    pub deleted: usize,
    /// Objects that were already gone when the sweep reached them.
    pub absent: usize,
    /// Objects that would not delete. The pass carried on past each one, and
    /// the next pass sees them again.
    pub failed: usize,
}
