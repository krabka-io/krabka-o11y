use super::*;

/// The shard a single search job scans: the live hot tier, or one cold block
/// narrowed to a half-open row-group range `[start, end)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JobShard {
    Live,
    Block {
        block_id: String,
        row_group_start: u32,
        row_group_end: u32,
    },
}
