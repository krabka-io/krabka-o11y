use super::JobShard;

/// The ownership key of a cold-block shard.
///
/// It names the block and the row-group range, so the two halves of a split
/// block are owned independently and a large block's work spreads instead of
/// landing whole on one querier.
pub(crate) fn shard_key(shard: &JobShard) -> String {
    match shard {
        JobShard::Live => "live".to_string(),
        JobShard::Block {
            block_id,
            row_group_start,
            row_group_end,
        } => format!("{block_id}#{row_group_start}-{row_group_end}"),
    }
}
