use super::{BTreeSet, Mutex, MutexGuard, PoisonError};

/// Blocks a writer has registered in its own in-memory index but has not yet
/// folded into a durable snapshot.
///
/// A snapshot write contributes only these. Every other block the writer's
/// index names is already in the snapshot chain, so the merge base carries it
/// forward on its own, and re-adding it is exactly how a block a *concurrent*
/// writer removed comes back from the dead. A block builder that loaded a
/// snapshot at startup, has held it in memory since, and still names a block
/// the compactor has replaced would otherwise union that block back in on its
/// next save: it has no removal to replay, because it never made one, so the
/// merge would restore the compaction's input beside the compaction's output
/// and the querier would read both.
///
/// Publication is the only thing that moves a block out of this set, and a
/// block is published only once a snapshot write that contributed it has
/// landed. A write that loses the conditional create publishes nothing.
///
/// Object keys are unique across tenants, so the set is flat.
///
/// Nothing is persisted: a deserialised index has published every block it
/// names, which is what the empty default says.
///
/// Interior mutability keeps the set clearable from `&self`, which is all a
/// save has.
#[derive(Default)]
pub(crate) struct PendingBlockAdditions {
    keys: Mutex<BTreeSet<String>>,
}

impl PendingBlockAdditions {
    /// Records that `object_key` was registered and is not yet durable.
    pub(crate) fn record(&self, object_key: &str) {
        self.lock().insert(object_key.to_string());
    }

    /// Drops a block that left the index before it was ever published, so the
    /// set does not accumulate keys no merge will ever look at.
    pub(crate) fn forget(&self, object_key: &str) {
        self.lock().remove(object_key);
    }

    /// The blocks a snapshot write must contribute.
    pub(crate) fn pending(&self) -> BTreeSet<String> {
        self.lock().clone()
    }

    /// Drops the additions a snapshot write has now made durable.
    pub(crate) fn commit(&self, applied: &BTreeSet<String>) {
        let mut guard = self.lock();
        for key in applied {
            guard.remove(key);
        }
    }

    fn lock(&self) -> MutexGuard<'_, BTreeSet<String>> {
        // A panic while holding the lock leaves the set intact: every method
        // here is a plain set edit. Recovering beats poisoning the whole index.
        self.keys.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
