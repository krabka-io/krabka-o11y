use super::*;

/// Fan one block into row-group-range jobs of about `target_per_job` each.
pub(crate) fn plan_block_jobs(block: &BlockMetaInfo, target_per_job: ByteSize) -> Vec<JobShard> {
    // A whole-block job when sizing is disabled, the block has <=1 row-group, or
    // it fits under the budget.
    if target_per_job == <ByteSize as ByteSizeExt>::ZERO
        || block.row_groups.len() <= 1
        || block.total() <= target_per_job
    {
        let end = block
            .row_groups
            .last()
            .map_or(1, |rg| rg.index.saturating_add(1));
        let start = block.row_groups.first().map_or(0, |rg| rg.index);
        return vec![JobShard::Block {
            block_id: block.block_id.clone(),
            row_group_start: start,
            row_group_end: end,
        }];
    }

    let mut jobs = Vec::new();
    let mut range_start: Option<u32> = None;
    let mut range_end = 0u32;
    let mut accumulated = <ByteSize as ByteSizeExt>::ZERO;
    for rg in &block.row_groups {
        range_start.get_or_insert(rg.index);
        range_end = rg.index.saturating_add(1);
        accumulated += rg.compressed;
        if accumulated >= target_per_job {
            jobs.push(JobShard::Block {
                block_id: block.block_id.clone(),
                row_group_start: range_start.take().unwrap_or(rg.index),
                row_group_end: range_end,
            });
            accumulated = <ByteSize as ByteSizeExt>::ZERO;
        }
    }
    if let Some(start) = range_start {
        jobs.push(JobShard::Block {
            block_id: block.block_id.clone(),
            row_group_start: start,
            row_group_end: range_end,
        });
    }
    jobs
}
