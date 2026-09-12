/// One object a deletion pass could not delete, and why.
///
/// The block is named as well as the object, because a sidecar that will not
/// delete is the block's problem: the caller has to decide whether to drop the
/// block from its index while an object of the block's still exists.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockDeletionFailure {
    /// The block the failed object belongs to.
    pub object_key: String,
    /// The object that would not delete: the block itself, or one of its
    /// sidecars.
    pub failed_key: String,
    /// The object store's own message.
    pub error: String,
}
