use krabka_units::{convert::TimeExt as _, fmt::Human as _};

use super::{
    CancellationToken, Cli, build_object_store, compaction_policy_from_cli, run_compactor_once,
};

/// Runs compaction passes on a schedule until shutdown.
///
/// Compaction is not something an operator types a time window into: the
/// planner decides what merges from the levels and time ranges the index
/// already records, and a pass that plans nothing costs one index load.
pub(crate) async fn run_compactor(
    cli: Cli,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let configured = build_object_store(&cli)?;
    let policy = compaction_policy_from_cli(&cli);
    tracing::info!(
        interval = %cli.compaction_interval.human(),
        max_blocks_per_job = policy.max_blocks_per_job(),
        target_rows = policy.target_rows_per_block(),
        max_level = %policy.max_level(),
        "traces compactor scheduling passes"
    );
    let mut tick = tokio::time::interval(cli.compaction_interval.to_std());
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return Ok(()),
            _ = tick.tick() => {}
        }
        match run_compactor_once(&cli, &configured, policy).await {
            Ok(compacted_blocks) => {
                tracing::info!(compacted_blocks, "traces compactor finished one pass");
            }
            // One failed pass is not a reason to lose the role. The next tick
            // reloads the index and replans from whatever is durable.
            Err(error) => {
                tracing::warn!(%error, "traces compaction pass failed; retrying on the next tick");
            }
        }
    }
}
