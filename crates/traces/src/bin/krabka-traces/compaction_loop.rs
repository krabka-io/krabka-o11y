use krabka_units::{Time, convert::TimeExt as _};

use super::{
    CancellationToken, Cli, CompactionPolicy, ConfiguredObjectStore, OverridesProvider,
    ServiceMetrics, run_compactor_once,
};

/// Runs compaction passes on `--compaction-interval` until `shutdown` fires.
///
/// Everything a pass needs is owned rather than borrowed, because the role
/// supervises this loop rather than awaiting it inline: a supervised task holds
/// its own state for as long as it runs. See
/// [`run_compactor`](super::run_compactor::run_compactor) for what supervision
/// buys.
pub(crate) async fn compaction_loop(
    cli: Cli,
    configured: ConfiguredObjectStore,
    policy: CompactionPolicy,
    overrides: OverridesProvider,
    metrics: ServiceMetrics,
    shutdown: CancellationToken,
) {
    let mut tick = tokio::time::interval(cli.compaction_interval.to_std());
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => return,
            _ = tick.tick() => {}
        }
        let started = std::time::Instant::now();
        let outcome = run_compactor_once(&cli, &configured, policy, &overrides, &metrics).await;
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
