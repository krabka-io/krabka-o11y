use krabka_observability::{CriticalTaskError, RoleReadiness, SupervisedTasks};
use krabka_units::fmt::Human as _;

use super::{
    CancellationToken, Cli, ServiceMetrics, SharedObjectStore, compaction_loop,
    compaction_policy_from_cli, limits_from_cli, load_traces_limits_overrides_config,
};

/// Runs compaction passes on a schedule until shutdown.
///
/// Compaction is not something an operator types a time window into: the
/// planner decides what merges from the levels and time ranges the index
/// already records, and a pass that plans nothing costs one index load.
///
/// Retention is the one thing here an operator does configure, per tenant, so
/// the role builds the [`OverridesProvider`] the sweep reads its windows from.
///
/// The schedule is [`compaction_loop`], and it is a supervised task rather than
/// the body of this function. The loop is the whole role, so a loop that
/// panicked, or that returned while the process was still up, would leave a
/// compactor that keeps its readiness gate and its `/metrics` endpoint and
/// compacts nothing. Neither probe can tell that from a compactor with nothing
/// to do. [`SupervisedTasks`] names the loop, reports either stop as a
/// [`CriticalTaskError`], and drains it on the way out.
///
/// [`OverridesProvider`]: krabka_traces::limits::OverridesProvider
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
    // Built before the store is reached, so that a malformed overrides file
    // stops the role at once rather than after its first pass has already
    // deleted blocks under the wrong window.
    let overrides = load_traces_limits_overrides_config(
        cli.traces_limits_overrides_config.as_deref(),
        limits_from_cli(&cli),
    )?;
    let configured = object_store.get(&cli, metrics.object_store.clone()).await?;
    object_store_gate.mark_ready();
    let policy = compaction_policy_from_cli(&cli);
    tracing::info!(
        interval = %cli.compaction_interval.human(),
        max_blocks_per_job = policy.max_blocks_per_job(),
        target_rows = policy.target_rows_per_block(),
        max_level = %policy.max_level(),
        default_block_retention = %cli.block_retention.human(),
        "traces compactor scheduling passes"
    );
    let mut tasks = SupervisedTasks::new(shutdown.clone());
    tasks.spawn(
        "traces compactor pass scheduler",
        compaction_loop(
            cli,
            configured,
            policy,
            overrides,
            metrics,
            shutdown.clone(),
        ),
    );
    let outcome = tokio::select! {
        () = shutdown.cancelled() => Ok(()),
        name = tasks.first_unexpected_exit() => Err(
            Box::<dyn std::error::Error + Send + Sync>::from(CriticalTaskError(name)),
        ),
    };
    // A pass already under way finishes before this returns, so `--target all`
    // stops this stage and moves to the next one rather than leaving a loop
    // running behind the drain.
    tasks.shutdown().await;
    outcome
}
