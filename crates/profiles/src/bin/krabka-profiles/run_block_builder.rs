use krabka_observability::RoleReadiness;

use super::{
    CancellationToken, Cli, ClientSecurity, ServiceMetrics, block_builder_config,
    build_object_store,
};

/// Consumes the profiles WAL and writes blocks, symbol databases and index
/// snapshots to object storage. `wal_security` is the TLS and SASL of the WAL
/// consumer.
///
/// # Errors
/// Returns an error when the object store cannot be built from
/// `--object-store-url`, or when the consume-and-flush loop fails.
pub(crate) async fn run_block_builder(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
    wal_security: Option<ClientSecurity>,
) -> Result<(), Box<dyn std::error::Error>> {
    let object_store_gate = readiness.gate("object-store");
    let configured = build_object_store(&cli.object_store_url, metrics.object_store.clone())
        .map_err(|e| format!("object store: {e}"))?;
    object_store_gate.mark_ready();
    let index_key = configured.object_key(&cli.index_object_key);
    let config = block_builder_config(&cli, configured.store, index_key, metrics, wal_security);
    krabka_profiles::blockbuilder::run_with_config(config, shutdown).await?;
    Ok(())
}
