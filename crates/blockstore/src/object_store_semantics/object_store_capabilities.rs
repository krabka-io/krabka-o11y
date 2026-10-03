/// What [`verify_object_store_semantics`](super::verify_object_store_semantics)
/// found that a store can do, beyond what it requires.
///
/// Create-if-absent, a bounded ranged read, list-after-write and an idempotent
/// delete are not fields. The probe fails when any one of them is missing, so a
/// value of this type means the store has all four.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ObjectStoreCapabilities {
    /// A write conditional on the current `ETag` or version succeeds, and one
    /// conditional on a stale `ETag` or version fails with a precondition error.
    pub conditional_update: bool,
    /// A read of the last `n` bytes works without the object's size. Azure
    /// Blob Storage refuses it, and the block reader then reads the size first.
    pub suffix_range_read: bool,
}
