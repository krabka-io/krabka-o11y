use super::{Deserialize, Serialize};

/// A block a writer has dropped, and enough about the record it dropped to
/// find it again in a published index and to prove it is the same one.
///
/// The fingerprint is the pin: see [`super::PendingBlockRemovals`]. The span
/// is what says *where* to look. A manifest-backed index is many objects cut
/// on a time grid, and a merge that replayed a removal without knowing the
/// retired record's span would have to fetch every shard of the tenant to find
/// it, which is the whole-index read the manifest exists to avoid.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PendingRemoval {
    pub(crate) fingerprint: u64,
    pub(crate) min_ts: i64,
    pub(crate) max_ts: i64,
}
