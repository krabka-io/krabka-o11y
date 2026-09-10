use super::{BTreeMap, BlockEntry, Deserialize, Serialize, SeriesFingerprint};

/// The persisted shape of a [`super::BlockList`].
///
/// Only the two structures that carry information are stored: the blocks in
/// ordinal order, and the inverted fingerprint postings that reference them by
/// ordinal. The time ordering, the key lookup and the running maximum are
/// derived, and rebuilding them on load is cheaper than reading them.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct BlockListRepr {
    pub(crate) entries: Vec<BlockEntry>,
    pub(crate) postings: BTreeMap<SeriesFingerprint, Vec<u32>>,
}
