use super::BlockDeletionFailure;

/// What one pass of [`delete_blocks`](super::delete_blocks) did.
///
/// The counts are per object rather than per block, because a block and its
/// sidecars can land differently: a block deletes and its sidecar is already
/// gone, or the block deletes and its sidecar refuses.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BlockDeletionReport {
    /// Block objects the store reported deleted.
    pub blocks_deleted: usize,
    /// Sidecar objects the store reported deleted.
    pub sidecars_deleted: usize,
    /// Objects that were already gone. A repeated sweep and a torn earlier
    /// pass both land here, and neither is a fault.
    pub objects_absent: usize,
    /// Objects that would not delete. The pass carried on past each one.
    pub failures: Vec<BlockDeletionFailure>,
}
