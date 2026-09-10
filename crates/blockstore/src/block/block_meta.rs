use super::{BlockLevel, Deserialize, Serialize, SeriesFingerprint};

/// Metadata recorded for each written block.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockMeta {
    pub tenant: String,
    pub object_key: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub row_count: usize,
    pub fingerprints: Vec<SeriesFingerprint>,
    /// How many rounds of compaction produced the block.
    ///
    /// A writer stamps [`BlockLevel::INGESTED`] on everything it closes: it
    /// encodes rows and has no way to know what they came from. Only an index
    /// does, because the only way to register a compaction output is to name
    /// the blocks it replaces, and the index promotes the record it stores as
    /// it retires them. See
    /// [`ProfileIndex::replace_profile_blocks`](crate::ProfileIndex::replace_profile_blocks).
    pub level: BlockLevel,
}
