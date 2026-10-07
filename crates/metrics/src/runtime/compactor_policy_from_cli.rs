use super::{BlockLevel, BlockTimestampUnit, Cli, CompactionPolicy};

/// Reads the compaction policy the operator configured.
///
/// The unit is not configurable: a metric block counts its timestamps in epoch
/// milliseconds, so that is the unit the policy converts its window into. A
/// nanosecond window over millisecond timestamps would put every block in one
/// bucket, and it would do so silently.
pub(crate) fn compactor_policy_from_cli(cli: &Cli) -> CompactionPolicy {
    CompactionPolicy::new(
        cli.compactor_max_blocks_per_job,
        cli.compactor_target_rows,
        BlockLevel(cli.compactor_max_level),
        cli.compactor_level_window,
        BlockTimestampUnit::Millis,
    )
}
