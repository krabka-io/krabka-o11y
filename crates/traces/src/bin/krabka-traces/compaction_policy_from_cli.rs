use super::{BlockLevel, BlockTimestampUnit, Cli, CompactionPolicy};

/// Reads the compaction policy the operator configured.
///
/// The unit is not configurable: a traces block counts its timestamps in epoch
/// nanoseconds, so that is the unit the policy converts its window into.
pub(crate) fn compaction_policy_from_cli(cli: &Cli) -> CompactionPolicy {
    CompactionPolicy::new(
        cli.compaction_max_blocks_per_job,
        cli.compaction_target_rows,
        BlockLevel(cli.compaction_max_level),
        cli.compaction_level_window,
        BlockTimestampUnit::Nanos,
    )
}
