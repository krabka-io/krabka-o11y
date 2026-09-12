use super::{BlockDeletionFailure, BlockDeletionReport};

/// What one ordered phase of an object-deletion pass did.
///
/// The metrics passes delete in two phases, and the phases delete different
/// kinds of object: first the `.index` manifests, then the block objects those
/// manifests named. One report across both could not say which of the two an
/// object belonged to, so each phase keeps its own.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompactionRetentionPhase {
    /// Objects the store reported deleted.
    pub deleted: usize,
    /// Objects that were already gone. A repeated pass and a torn earlier pass
    /// both land here, and neither is a fault.
    pub absent: usize,
    /// Objects that would not delete. The phase carried on past each one.
    pub failures: Vec<BlockDeletionFailure>,
}

impl From<BlockDeletionReport> for CompactionRetentionPhase {
    // One object per deletion and no sidecars, because the phases are what
    // order the block against its manifest: a phase that let
    // `delete_blocks` take a sidecar would delete the two in the order that
    // function chooses rather than in the order metrics needs.
    fn from(report: BlockDeletionReport) -> Self {
        Self {
            deleted: report.blocks_deleted + report.sidecars_deleted,
            absent: report.objects_absent,
            failures: report.failures,
        }
    }
}
