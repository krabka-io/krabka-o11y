use super::BlockLevel;

/// One merge the planner asks for: these blocks become one block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactionJob {
    pub tenant: String,
    /// Always at least two blocks, all at the same level. Merging one block
    /// would rewrite it for nothing.
    pub input_keys: Vec<String>,
    /// The level of the block this job writes: one above its inputs'.
    pub output_level: BlockLevel,
    pub min_ts: i64,
    pub max_ts: i64,
    /// The inputs' rows summed, as far as the index knows them.
    pub row_count: usize,
}
