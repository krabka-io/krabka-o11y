use krabka_units::convert::TimeExt as _;

use super::{BlockLevel, Cli, CompactionPolicy};

/// Reads the compaction policy the operator configured.
pub(crate) fn compaction_policy_from_cli(cli: &Cli) -> CompactionPolicy {
    CompactionPolicy::new(
        cli.compactor_max_blocks_per_job,
        cli.compactor_target_rows,
        BlockLevel(cli.compactor_max_level),
        cli.compactor_level_window.nanos_i64(),
    )
}
