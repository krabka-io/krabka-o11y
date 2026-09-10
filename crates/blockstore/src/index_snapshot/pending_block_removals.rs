use super::{BTreeMap, Mutex, MutexGuard, PendingRemoval, PoisonError};

/// Blocks a writer has dropped from its own in-memory index but has not yet
/// folded into a durable snapshot, each pinned to the record it dropped.
///
/// A snapshot write merges the writer's index into the newest stored one, and a
/// plain union would resurrect everything the writer removed, because the merge
/// base still names those blocks. Replaying the removals against the base is
/// what makes a compaction swap survive the merge.
///
/// The fingerprint is what keeps that replay from becoming data loss in the
/// other direction. Object keys are *derived*, not minted, so the same key can
/// legitimately be written again, by this writer or another one, after this
/// writer dropped it. A removal that named the key alone would hide that live
/// block, and hide it for good once the base carries it forward. A removal that
/// also names the record it retired simply fails to match the new one, so the
/// live block stays. Fingerprints are only ever compared inside one process,
/// between a record this writer held and the record the merge base carries, so
/// they never have to be stable across builds or across hosts.
///
/// The retired record's time span travels with it for a separate reason: the
/// published index is cut on a time grid, and the span is what tells the merge
/// which shards of it to fetch. See [`PendingRemoval`].
///
/// Nothing is persisted: once a write lands, the base no longer names the
/// removed blocks, so the set is cleared.
///
/// Interior mutability keeps the set clearable from `&self`, which is all a
/// save has.
#[derive(Default)]
pub(crate) struct PendingBlockRemovals {
    by_tenant: Mutex<BTreeMap<String, BTreeMap<String, PendingRemoval>>>,
}

impl PendingBlockRemovals {
    /// Records that the `(object key, retired record)` pairs in `blocks` no
    /// longer belong to `tenant`.
    pub(crate) fn record<'key>(
        &self,
        tenant: &str,
        blocks: impl IntoIterator<Item = (&'key str, PendingRemoval)>,
    ) {
        let mut guard = self.lock();
        let pending = guard.entry(tenant.to_string()).or_default();
        for (key, removal) in blocks {
            pending.insert(key.to_string(), removal);
        }
        if pending.is_empty() {
            guard.remove(tenant);
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

    /// The removals a snapshot write must replay, as object key to the record
    /// that was removed.
    pub(crate) fn pending(&self) -> BTreeMap<String, BTreeMap<String, PendingRemoval>> {
        self.lock().clone()
    }

    /// Drops the removals a snapshot write has now made durable.
    ///
    /// A removal re-recorded against a different record while the write was in
    /// flight retires a different record, so it survives the commit.
    pub(crate) fn commit(&self, applied: &BTreeMap<String, BTreeMap<String, PendingRemoval>>) {
        let mut guard = self.lock();
        for (tenant, blocks) in applied {
            if let Some(pending) = guard.get_mut(tenant) {
                for (key, removal) in blocks {
                    if pending.get(key) == Some(removal) {
                        pending.remove(key);
                    }
                }
                if pending.is_empty() {
                    guard.remove(tenant);
                }
            }
        }
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<String, BTreeMap<String, PendingRemoval>>> {
        // A panic while holding the lock leaves the set intact: every method
        // here is a plain map edit. Recovering beats poisoning the whole index.
        self.by_tenant
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}
