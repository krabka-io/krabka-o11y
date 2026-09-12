/// The input blocks a compaction pass retired, for a later pass to delete.
///
/// A merge retires its inputs by deleting their `.index` manifests, which is
/// the whole of the metrics index. The block objects themselves stay, and this
/// is where their keys wait.
///
/// # Why the deletion waits
///
/// `krabka-metrics-service` serves a cold block index out of a cache for
/// `--cold-cache-ttl`, so a querier can hold an index that names a retired
/// input for that long after the manifest is gone. Deleting the object in the
/// same pass that retired it would leave that querier resolving a key with
/// nothing behind it. Waiting for a later pass costs one compaction interval of
/// storage and removes the window.
///
/// The queue is in memory, and losing it loses nothing: an input no manifest
/// names is an orphan, and the retention pass's orphan sweep reclaims it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DeferredBlockDeletions {
    keys: Vec<String>,
}

impl DeferredBlockDeletions {
    /// An empty queue, as a compactor starts with.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The block keys still waiting to be deleted.
    #[must_use]
    pub fn keys(&self) -> &[String] {
        &self.keys
    }

    /// Whether the queue holds nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Empties the queue and answers with what it held.
    pub(crate) fn take(&mut self) -> Vec<String> {
        std::mem::take(&mut self.keys)
    }

    /// Adds the blocks one pass retired.
    pub(crate) fn extend(&mut self, keys: impl IntoIterator<Item = String>) {
        self.keys.extend(keys);
    }
}
