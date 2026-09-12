use krabka_units::{convert::TimeExt as _, fmt::Human as _};

use super::{
    Arc, CancellationToken, Cli, DownsamplePolicy, ObjectStore, OverridesProvider, ServiceMetrics,
    Time, compaction_policy_from_cli, run_compaction_pass,
};

/// Runs compaction passes on `--compactor-interval` until `shutdown` fires.
///
/// `store` and `index_key` are parameters rather than read from `cli` because
/// `--target all` builds one object store for the whole process: a compactor
/// that parsed `--object-store-url` again would rearrange blocks in a store
/// nothing else uses. `overrides` is a parameter for the same reason: the
/// ingest and query roles resolve a tenant's limits through the one provider
/// the process loaded, and the retention window a pass deletes by is one of
/// those limits.
///
/// Everything a pass needs is owned rather than borrowed, because the role
/// supervises this loop rather than awaiting it inline. See
/// [`run_compactor`](super::run_compactor::run_compactor) for what supervision
/// buys.
pub(crate) async fn compaction_loop(
    cli: Arc<Cli>,
    store: Arc<dyn ObjectStore>,
    index_key: String,
    overrides: OverridesProvider,
    metrics: ServiceMetrics,
    shutdown: CancellationToken,
) {
    let policy = compaction_policy_from_cli(&cli);
    let downsample = cli
        .compactor_downsample_resolution
        .map(|resolution| DownsamplePolicy {
            resolution_ns: resolution.nanos_i64(),
        });
    tracing::info!(
        interval = %cli.compactor_interval.human(),
        max_blocks_per_job = policy.max_blocks_per_job(),
        target_rows = policy.target_rows_per_block(),
        max_level = %policy.max_level(),
        "profiles compactor scheduling passes"
    );
    // A pass reloads the index rather than carrying one across ticks: the
    // block builder publishes new blocks into the same snapshot chain, and a
    // stale in-memory copy would plan against blocks that have since been
    // replaced.
    let mut tick = tokio::time::interval(cli.compactor_interval.to_std());
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            _ = tick.tick() => {}
        }
        let started = std::time::Instant::now();
        let outcome = run_compaction_pass(
            &store, &index_key, &cli, policy, downsample, &overrides, &metrics,
        )
        .await;
        metrics
            .compaction
            .record_run(outcome.is_ok(), Time::from_std(started.elapsed()));
        match outcome {
            Ok(compacted_blocks) => tracing::info!(
                compacted_blocks,
                downsample_resolution = ?cli.compactor_downsample_resolution,
                "profiles compactor finished one pass"
            ),
            // One failed pass is not a reason to lose the role. The next tick
            // reloads the index and replans from whatever is durable. The
            // counter is what makes that visible: without it a role whose
            // every pass fails exports what a role with nothing to do exports.
            Err(error) => {
                tracing::warn!(%error, "profiles compaction pass failed; retrying on the next tick");
            }
        }
    }
}
