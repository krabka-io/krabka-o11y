use krabka_observability::{CriticalTaskError, RoleReadiness, SupervisedTasks};

use super::{
    Arc, CancellationToken, Cli, ServiceMetrics, build_object_store, compaction_loop,
    load_profiles_limits_overrides_config,
};

/// Merges blocks that are already in object storage into larger ones, and
/// deletes the blocks nothing needs any more.
///
/// This role reaches no broker: it reads and writes object storage only, which
/// is what separates it from the block builder.
///
/// The schedule is [`compaction_loop`], and it is a supervised task rather
/// than the body of this function. The loop is the whole role, so a loop that
/// panicked, or that returned while the process was still up, would leave a
/// compactor that keeps its readiness gate and its `/metrics` endpoint and
/// compacts nothing. Neither probe can tell that from a compactor with nothing
/// to do. [`SupervisedTasks`] names the loop, reports either stop as a
/// [`CriticalTaskError`], and drains it on the way out.
///
/// # Errors
/// Returns an error when the object store cannot be built from
/// `--object-store-url`, when the limits file cannot be read, and when the
/// loop stops on its own while the process is serving.
pub(crate) async fn run_compactor(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    let object_store_gate = readiness.gate("object-store");
    let configured = build_object_store(&cli.object_store_url, metrics.object_store.clone())
        .map_err(|e| format!("object store: {e}"))?;
    object_store_gate.mark_ready();
    let index_key = cli.index_object_key.clone();
    // The same file the ingest and query roles read. Retention is a per-tenant
    // limit, so a compactor that read no overrides would keep every block of
    // every tenant forever.
    let overrides =
        load_profiles_limits_overrides_config(cli.profiles_limits_overrides_config.as_deref())?;
    let mut tasks = SupervisedTasks::new(shutdown.clone());
    tasks.spawn(
        "profiles compactor pass scheduler",
        compaction_loop(
            Arc::new(cli),
            configured.store,
            index_key,
            overrides,
            metrics,
            shutdown.clone(),
        ),
    );
    let outcome = tokio::select! {
        () = shutdown.cancelled() => Ok(()),
        name = tasks.first_unexpected_exit() => {
            Err(Box::<dyn std::error::Error>::from(CriticalTaskError(name)))
        }
    };
    // A pass already under way finishes before this returns, so `--target all`
    // stops this stage and moves to the next one rather than leaving a loop
    // running behind the drain.
    tasks.shutdown().await;
    outcome
}
