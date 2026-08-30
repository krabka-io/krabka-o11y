use super::*;

/// Push the querier's scan-job params for a cold-block shard. The live shard
/// sends no such params.
pub(crate) fn push_shard_params(params: &mut Vec<(&'static str, String)>, shard: &JobShard) {
    if let JobShard::Block {
        block_id,
        row_group_start,
        row_group_end,
    } = shard
    {
        params.push(("block", block_id.clone()));
        params.push(("rowGroupStart", row_group_start.to_string()));
        params.push(("rowGroupEnd", row_group_end.to_string()));
    }
}
