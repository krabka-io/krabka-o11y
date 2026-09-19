use krabka_o11y_verified::compaction_run_ends;

use super::{
    BTreeMap, BlockLevel, CompactionCandidate, CompactionJob, CompactionPolicy, job_from_run,
};

/// Groups compactable blocks into merge jobs.
///
/// Blocks meet only their own kind: same tenant, same level, same time bucket
/// at that level's window. Within a group the blocks are taken in time order
/// and accumulated until the run either reaches the fan-in cap or covers the
/// row target, and each closed run of two or more blocks becomes one job.
///
/// # Termination
///
/// Left alone with no new ingest, repeated plan-and-apply reaches an empty
/// plan. Give each live block the weight `max_level - level`, and the index
/// the sum of those weights. A job replaces `n >= 2` blocks of weight `w` with
/// one of weight `w - 1`, so the sum falls by at least `w + 1` -- strictly,
/// every time. The sum is a non-negative integer, so only finitely many jobs
/// can run. Two rules do the work: an input must sit below `max_level`, and a
/// job must have at least two inputs.
///
/// The row target is the second half of the answer to "what stops it
/// rewriting the same data forever". A block that already holds
/// `target_rows_per_block` rows is skipped outright, so a large block is
/// written once and then never read again by the planner, whatever its level.
#[must_use]
pub fn plan_compactions(
    candidates: &[CompactionCandidate],
    policy: CompactionPolicy,
) -> Vec<CompactionJob> {
    let mut groups: BTreeMap<(&str, u32, i64), Vec<&CompactionCandidate>> = BTreeMap::new();
    for candidate in candidates {
        if candidate.level >= policy.max_level()
            || candidate.row_count >= policy.target_rows_per_block()
        {
            continue;
        }
        let bucket = candidate
            .min_ts
            .div_euclid(policy.window_ticks_for(candidate.level));
        groups
            .entry((candidate.tenant.as_str(), candidate.level.get(), bucket))
            .or_default()
            .push(candidate);
    }

    let mut jobs = Vec::new();
    for ((tenant, level, _bucket), mut blocks) in groups {
        let level = BlockLevel(level);
        blocks.sort_by(|left, right| {
            left.min_ts
                .cmp(&right.min_ts)
                .then_with(|| left.max_ts.cmp(&right.max_ts))
                .then_with(|| left.object_key.cmp(&right.object_key))
        });
        let row_counts = blocks
            .iter()
            .map(|candidate| candidate.row_count)
            .collect::<Vec<_>>();
        let mut start = 0;
        for end in compaction_run_ends(
            &row_counts,
            policy.max_blocks_per_job(),
            policy.target_rows_per_block(),
        ) {
            let rows = row_counts[start..end]
                .iter()
                .fold(0_usize, |sum, rows| sum.saturating_add(*rows));
            jobs.extend(job_from_run(tenant, level, &blocks[start..end], rows));
            start = end;
        }
    }
    jobs
}
