use krabka_observability::{CancellationToken, CriticalTaskError, SupervisedTasks};
use krabka_units::fmt::Human as _;

use super::{
    BlockWriter, Cli, ObjectStoreCompactionIndexSink, RoleReadiness, ServiceMetrics,
    build_object_store, compactor_loop, compactor_policy_from_cli,
};

/// Runs compaction passes on a schedule until shutdown.
///
/// Compaction is not something an operator types a time window into: the
/// planner decides what merges from the levels and time ranges the `.index`
/// manifests already record, and a pass that plans nothing costs one listing.
///
/// The compactor reaches no broker. It reads and writes object storage only,
/// which is what separates it from the block builder, so the object store is
/// its one readiness gate. Each pass reads the manifests itself, so there is no
/// index gate to hold: a pass that cannot read one fails and the next tick
/// retries.
///
/// The schedule is [`compactor_loop`], and it is a supervised task rather than
/// the body of this function. The loop is the whole role, so a loop that
/// panicked, or that returned while the process was still up, would leave a
/// compactor that keeps its readiness gate and its `/metrics` endpoint and
/// compacts nothing. Neither probe can tell that from a compactor with nothing
/// to do. [`SupervisedTasks`] names the loop, reports either stop as a
/// [`CriticalTaskError`], and drains it on the way out.
// cargo-mutants: live compactor I/O wiring is covered by integration workflows.
#[cfg_attr(test, mutants::skip)]
pub(crate) async fn run_compactor(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
) -> Result<(), Box<dyn std::error::Error>> {
    let object_store_gate = readiness.gate("object-store");
    let store = build_object_store(&cli.object_store_url, metrics.object_store.clone())?;
    object_store_gate.mark_ready();
    let policy = compactor_policy_from_cli(&cli);
    tracing::info!(
        interval = %cli.compactor_interval.human(),
        max_blocks_per_job = policy.max_blocks_per_job(),
        target_rows = policy.target_rows_per_block(),
        max_level = %policy.max_level(),
        level_window = %policy.level_window().human(),
        "metrics compactor scheduling passes"
    );
    let stopping = CancellationToken::new();
    let signal = stopping.clone();
    // Not supervised: this task is meant to finish, and finishing is how it
    // does its job.
    tokio::spawn(async move {
        krabka_observability::shutdown_signal().await;
        signal.cancel();
    });
    let mut tasks = SupervisedTasks::new(stopping.clone());
    tasks.spawn(
        "metrics compactor pass scheduler",
        compactor_loop(
            store.clone(),
            BlockWriter::new(store.clone()).with_metrics(metrics.object_store.clone()),
            ObjectStoreCompactionIndexSink::new(store),
            policy,
            cli.compactor_interval,
            metrics,
            stopping.clone(),
        ),
    );
    let outcome = tokio::select! {
        () = stopping.cancelled() => Ok(()),
        name = tasks.first_unexpected_exit() => Err(
            Box::<dyn std::error::Error>::from(CriticalTaskError(name)),
        ),
    };
    // A pass already under way finishes before this returns, so the process
    // never drops a half-written merge on the way out.
    tasks.shutdown().await;
    outcome
}
