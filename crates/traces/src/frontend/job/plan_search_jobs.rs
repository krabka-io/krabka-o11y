use super::*;

/// Plan search jobs for a query window that ends at `query_end_ns`, over the
/// candidate blocks and the hot/cold frontier.
///
/// - One `Live` job if and only if the query window reaches the hot tier, that
///   is `query_end_ns >= hot_frontier_ns`.
/// - For each block, one whole-block job if it fits the budget. If it does not,
///   one row-group-range job per chunk of about `target_per_job`.
#[must_use]
pub fn plan_search_jobs(
    blocks: &[BlockMetaInfo],
    query_end_ns: i64,
    hot_frontier_ns: i64,
    target_per_job: ByteSize,
) -> JobPlan {
    let mut jobs = Vec::new();
    if query_end_ns >= hot_frontier_ns {
        jobs.push(JobShard::Live);
    }
    for b in blocks {
        jobs.extend(plan_block_jobs(b, target_per_job));
    }
    JobPlan {
        jobs,
        total_blocks: blocks.len() as u64,
    }
}
