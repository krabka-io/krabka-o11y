use super::SeriesFingerprint;

/// Parsed `N_of_M` Mimir query shard selector.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryShardSelector {
    pub index: usize,
    pub total: usize,
}

impl QueryShardSelector {
    /// Whether series fingerprint `fp` falls in this shard.
    ///
    /// This shards on the crate's internal FNV [`SeriesFingerprint`], a
    /// 0-based remap of Mimir's 1-based `N_of_M`. It is self-consistent within
    /// Krabka, but it is **not** byte-compatible with Mimir's stable
    /// label-hash sharding, which hashes the label set with a different
    /// algorithm, so `__query_shard__` is an internal-only sharding scheme.
    /// It must never be exposed to, nor accepted from, a real Mimir
    /// client, because the shard boundaries would not agree across the two
    /// systems.
    #[must_use]
    pub fn matches(self, fp: SeriesFingerprint) -> bool {
        fp % self.total as u64 == (self.index - 1) as u64
    }
}
