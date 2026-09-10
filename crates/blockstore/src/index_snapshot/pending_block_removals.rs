use super::{BTreeMap, BTreeSet, Mutex, MutexGuard, PoisonError};

/// Block object keys a writer has dropped from its own in-memory index but has
/// not yet folded into a durable snapshot.
///
/// A snapshot write merges the writer's index into the newest stored one, and a
/// plain union would resurrect everything the writer removed, because the merge
/// base still names those blocks. Replaying the removals against the base is
/// what makes a compaction swap survive the merge. Nothing is persisted: once a
/// write lands, the base no longer names the removed blocks, so the set is
/// cleared.
///
/// Interior mutability keeps the set clearable from `&self`, which is all a
/// save has.
#[derive(Default)]
pub(crate) struct PendingBlockRemovals {
    by_tenant: Mutex<BTreeMap<String, BTreeSet<String>>>,
}

impl PendingBlockRemovals {
    /// Records that `keys` no longer belong to `tenant`.
    pub(crate) fn record<'key>(&self, tenant: &str, keys: impl IntoIterator<Item = &'key str>) {
        let mut guard = self.lock();
        let pending = guard.entry(tenant.to_string()).or_default();
        for key in keys {
            pending.insert(key.to_string());
        }
    }

    /// Cancels a pending removal, for when the same object key is written
    /// again. A compaction that reuses the key of a block it replaces relies on
    /// this: the block is live, so the merge must not drop it.
    pub(crate) fn forget(&self, tenant: &str, key: &str) {
        let mut guard = self.lock();
        if let Some(pending) = guard.get_mut(tenant) {
            pending.remove(key);
            if pending.is_empty() {
                guard.remove(tenant);
            }
        }
    }

    /// The removals a snapshot write must replay.
    pub(crate) fn pending(&self) -> BTreeMap<String, BTreeSet<String>> {
        self.lock().clone()
    }

    /// Drops the removals a snapshot write has now made durable.
    pub(crate) fn commit(&self, applied: &BTreeMap<String, BTreeSet<String>>) {
        let mut guard = self.lock();
        for (tenant, keys) in applied {
            if let Some(pending) = guard.get_mut(tenant) {
                for key in keys {
                    pending.remove(key);
                }
                if pending.is_empty() {
                    guard.remove(tenant);
                }
            }
        }
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<String, BTreeSet<String>>> {
        // A panic while holding the lock leaves the set intact: every method
        // here is a plain map edit. Recovering beats poisoning the whole index.
        self.by_tenant
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}
