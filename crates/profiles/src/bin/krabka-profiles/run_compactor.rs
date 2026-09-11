use krabka_observability::RoleReadiness;

use super::{Arc, CancellationToken, Cli, ServiceMetrics, build_object_store, compaction_loop};

/// Merges blocks that are already in object storage into larger ones.
///
/// This role reaches no broker: it reads and writes object storage only, which
/// is what separates it from the block builder.
///
/// # Errors
/// Returns an error when the object store cannot be built from
/// `--object-store-url`.
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
    let index_key = configured.object_key(&cli.index_object_key);
    compaction_loop(
        Arc::new(cli),
        configured.store,
        index_key,
        metrics,
        shutdown,
    )
    .await;
    Ok(())
}
