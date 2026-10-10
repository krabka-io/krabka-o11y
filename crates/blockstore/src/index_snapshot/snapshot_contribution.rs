use super::{Arc, BTreeMap, BTreeSet, ByteSize, ObjectStore, PendingRemoval};

/// What one save of a sharded index contributes, and where it goes: the
/// blocks this writer registered and the removals it owes the base.
pub(crate) struct SnapshotContribution<'a> {
    pub(crate) store: &'a Arc<dyn ObjectStore>,
    pub(crate) key: &'a str,
    pub(crate) removals: &'a BTreeMap<String, BTreeMap<String, PendingRemoval>>,
    pub(crate) additions: &'a BTreeSet<String>,
    pub(crate) max_bytes: ByteSize,
}
