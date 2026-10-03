/// What a role does with its object store, and so how much of it the startup
/// probe may exercise.
///
/// A role that only reads, such as a querier, can run with a credential that
/// has no put or delete. The write probe would fail on that credential, and
/// it would test semantics that the role never uses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectStoreAccess {
    /// The role writes and deletes objects. The probe checks every semantic
    /// with [`verify_object_store_semantics`](super::verify_object_store_semantics).
    ReadWrite,
    /// The role only lists and reads. The probe writes nothing, and checks
    /// what [`verify_object_store_read_access`](super::verify_object_store_read_access)
    /// checks.
    ReadOnly,
}
