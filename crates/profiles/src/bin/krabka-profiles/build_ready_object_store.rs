use krabka_observability::RoleReadiness;

use super::{Cli, ConfiguredObjectStore, ServiceMetrics, build_object_store};

pub(crate) async fn build_ready_object_store(
    cli: &Cli,
    metrics: &ServiceMetrics,
    readiness: &RoleReadiness,
) -> Result<(ConfiguredObjectStore, String), String> {
    let configured = build_object_store(&cli.object_store_url, metrics.object_store.clone())
        .await
        .map_err(|error| format!("object store: {error}"))?;
    readiness.gate("object-store").mark_ready();
    Ok((configured, cli.index_object_key.clone()))
}
