use super::{BTreeMap, BTreeSet, BlockLevel, Deserialize, Serialize, ShardedTraceBloom};

/// The per-block trace footprint registered by a block builder.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TraceBlockStats {
    pub object_key: String,
    pub min_ts: i64,
    pub max_ts: i64,
    pub bloom: ShardedTraceBloom,
    pub tag_names: BTreeSet<String>,
    pub tag_values: BTreeMap<String, BTreeSet<String>>,
    /// Rows in the block, as the writer counted them while encoding it.
    ///
    /// The compaction planner reads this to tell a block that has reached its
    /// size target, and so needs no further merging, from one that has not.
    pub row_count: usize,
    /// How many rounds of compaction produced the block. See
    /// [`BlockMeta::level`](crate::BlockMeta::level) for who sets it.
    pub level: BlockLevel,
}
