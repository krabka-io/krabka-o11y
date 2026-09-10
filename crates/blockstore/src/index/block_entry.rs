use super::{Deserialize, Serialize};

/// One block, as the index knows it.
///
/// The set of fingerprints the block holds is *not* here. It used to be, as a
/// `BTreeSet` per block, which made the index grow as blocks × cardinality and
/// made pruning a membership probe per block. The pairs now live once, inverted,
/// in [`super::BlockList`]; what stays behind is their count and an
/// order-independent digest, which is enough to tell a restatement of the same
/// block from a real change without materialising the set again.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BlockEntry {
    pub(crate) object_key: String,
    pub(crate) min_ts: i64,
    pub(crate) max_ts: i64,
    pub(crate) row_count: usize,
    pub(crate) fingerprint_count: usize,
    pub(crate) fingerprint_digest: u64,
}

impl BlockEntry {
    /// Whether the block's time span meets `[min_ts, max_ts]`. Inclusive at
    /// both ends, as every other overlap test in the crate is.
    pub(crate) const fn overlaps(&self, min_ts: i64, max_ts: i64) -> bool {
        self.min_ts <= max_ts && self.max_ts >= min_ts
    }
}
