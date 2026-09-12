/// Errors raised while a block lifecycle pass runs.
///
/// Deleting one object is not one of them. A sweep that stopped at the first
/// failed delete would leave the rest of the pass undone and retry the same
/// object forever, so [`delete_blocks`](super::delete_blocks) and
/// [`reconcile_orphans`](super::reconcile_orphans) report a failed delete and
/// carry on. Only a failure that leaves the pass unable to tell a live block
/// from a dead one reaches here.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LifecycleError {
    /// Listing the prefix failed, so the sweep never learned which objects
    /// exist. It cannot tell an orphan from a live block, and deleting on that
    /// basis would delete live blocks.
    #[error("object store error: {0}")]
    ObjectStore(String),
}

impl From<object_store::Error> for LifecycleError {
    fn from(error: object_store::Error) -> Self {
        Self::ObjectStore(error.to_string())
    }
}
