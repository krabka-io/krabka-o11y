use super::{BlockLevel, CompactionCandidate, CompactionJob};

/// Turns an accumulated run of blocks into a job, or into nothing.
///
/// A run of one is not a job: merging a block with itself rewrites it at a
/// cost and leaves the same rows behind, which is exactly the loop a planner
/// must not enter.
pub(crate) fn job_from_run(
    tenant: &str,
    level: BlockLevel,
    run: &[&CompactionCandidate],
    row_count: usize,
) -> Option<CompactionJob> {
    if run.len() < 2 {
        return None;
    }
    Some(CompactionJob {
        tenant: tenant.to_string(),
        input_keys: run
            .iter()
            .map(|candidate| candidate.object_key.clone())
            .collect(),
        output_level: level.next(),
        min_ts: run
            .iter()
            .map(|candidate| candidate.min_ts)
            .min()
            .unwrap_or_default(),
        max_ts: run
            .iter()
            .map(|candidate| candidate.max_ts)
            .max()
            .unwrap_or_default(),
        row_count,
    })
}
