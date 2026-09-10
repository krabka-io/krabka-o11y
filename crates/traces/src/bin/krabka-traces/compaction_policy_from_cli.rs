use krabka_units::convert::TimeExt as _;

use super::{BlockLevel, Cli, CompactionPolicy};

/// Reads the compaction policy the operator configured.
pub(crate) fn compaction_policy_from_cli(cli: &Cli) -> CompactionPolicy {
    CompactionPolicy::new(
        cli.compaction_max_blocks_per_job,
        cli.compaction_target_rows,
        BlockLevel(cli.compaction_max_level),
        cli.compaction_level_window.nanos_i64(),
    )
}
