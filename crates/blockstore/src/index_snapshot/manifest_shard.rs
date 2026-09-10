use super::{Deserialize, IndexShardRange, Serialize};

/// One shard payload, as the manifest names it.
///
/// The payload's object key is *derived* from the tenant, the span and the
/// content hash, so the manifest does not spell it out. Three numbers and a
/// hash cost about fifty bytes an entry, and a manifest is read and rewritten
/// on every flush, so the entry's width is the thing that decides how wide a
/// grid is affordable.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ManifestShard {
    /// First tick the shard covers, inclusive.
    #[serde(rename = "s")]
    pub(crate) start: i64,
    /// Last tick the shard covers, inclusive.
    #[serde(rename = "e")]
    pub(crate) end: i64,
    /// Hash of the payload bytes, lower-case hex. See
    /// [`super::shard_payload_content_hash`].
    #[serde(rename = "h")]
    pub(crate) content: String,
}

impl ManifestShard {
    pub(crate) const fn range(&self) -> IndexShardRange {
        IndexShardRange::new(self.start, self.end)
    }
}
