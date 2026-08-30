use super::*;

/// The output of planning: the jobs to dispatch, and how many blocks they
/// cover. The block count seeds `metrics.totalBlocks`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobPlan {
    pub jobs: Vec<JobShard>,
    pub total_blocks: u64,
}
