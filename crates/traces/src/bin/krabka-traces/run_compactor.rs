use krabka_observability::RoleReadiness;
use krabka_units::{Time, convert::TimeExt as _, fmt::Human as _};

use super::{
    CancellationToken, Cli, ServiceMetrics, SharedObjectStore, compaction_policy_from_cli,
    run_compactor_once,
};

/// Runs compaction passes on a schedule until shutdown.
///
/// Compaction is not something an operator types a time window into: the
/// planner decides what merges from the levels and time ranges the index
/// already records, and a pass that plans nothing costs one index load.
pub(crate) async fn run_compactor(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
    object_store: &SharedObjectStore,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // The one thing a compactor cannot work without. Each pass loads the index
    // itself, so there is no index gate to hold: a pass that cannot read one
    // fails and the next tick retries.
    let object_store_gate = readiness.gate("object-store");
    let configured = object_store.get(&cli, metrics.object_store.clone()).await?;
    object_store_gate.mark_ready();
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
        let started = std::time::Instant::now();
        let outcome = run_compactor_once(&cli, &configured, policy, &metrics).await;
        metrics
            .compaction
            .record_run(outcome.is_ok(), Time::from_std(started.elapsed()));
        match outcome {
            Ok(compacted_blocks) => {
                tracing::info!(compacted_blocks, "traces compactor finished one pass");
            }
            // One failed pass is not a reason to lose the role. The next tick
            // reloads the index and replans from whatever is durable. The
            // counter is what makes that visible: without it a role whose every
            // pass fails exports what a role with nothing to do exports.
            Err(error) => {
                tracing::warn!(%error, "traces compaction pass failed; retrying on the next tick");
            }
        }
    }
}
