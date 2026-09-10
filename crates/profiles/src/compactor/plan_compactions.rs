use super::{CompactionJob, CompactionPolicy, ProfileIndex, plan_level_compactions};

/// Plans the merges the profile index is due, under `policy`.
///
/// The planner itself is signal-agnostic: the index only has to say what
/// blocks it holds, at what level, over what time range, and how large they
/// are. See [`plan_level_compactions`] for why the plan terminates.
#[must_use]
pub fn plan_compactions(index: &ProfileIndex, policy: CompactionPolicy) -> Vec<CompactionJob> {
    plan_level_compactions(&index.compaction_candidates(), policy)
}
