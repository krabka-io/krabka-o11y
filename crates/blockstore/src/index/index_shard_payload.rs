use super::{BTreeMap, BTreeSet, BlockEntry, Labels, SeriesFingerprint};

/// One shard's worth of a tenant's index, ready to encode.
///
/// The caller gives the blocks in the order the encoder writes them, and
/// `postings` refers to them by their position in that order. It does not use
/// the ordinal a block holds in the live index. A shard is a document of its
/// own, so its ordinals start at zero.
pub(crate) struct IndexShardPayload<'index> {
    pub(crate) tenant: &'index str,
    pub(crate) series: &'index BTreeMap<SeriesFingerprint, Labels>,
    pub(crate) selected: BTreeSet<SeriesFingerprint>,
    pub(crate) blocks: Vec<&'index BlockEntry>,
    pub(crate) postings: BTreeMap<SeriesFingerprint, Vec<u32>>,
}
