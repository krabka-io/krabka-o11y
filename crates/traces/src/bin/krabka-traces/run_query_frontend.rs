use krabka_observability::RoleReadiness;
use krabka_units::{ByteSize, convert::ByteSizeExt as _};

use super::{
    BlockStoreGates, CancellationToken, Cli, ProcessSecurity, ServiceMetrics, SharedObjectStore,
    SocketAddr, build_trace_index_catalog, frontend, frontend_config_from_cli, limits_from_cli,
    load_traces_limits_overrides_config,
};

pub(crate) async fn run_query_frontend(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
    object_store: &SharedObjectStore,
    security: &ProcessSecurity,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = cli.listen.parse()?;
    let mut cfg = frontend_config_from_cli(&cli, addr)?;
    cfg.overrides = load_traces_limits_overrides_config(
        cli.traces_limits_overrides_config.as_deref(),
        limits_from_cli(&cli),
    )?;
    // The catalog is built before the listener binds; `querier-membership` is
    // the gate that keeps moving after it, and `frontend::run_query_frontend`
    // registers that one.
    let gates = (cli.target_bytes_per_job > ByteSize::from_bytes(0))
        .then(|| BlockStoreGates::register(&readiness));
    let catalog = build_trace_index_catalog(&cli, &metrics, gates.as_ref(), object_store).await?;
    tracing::info!(%addr, "traces query-frontend listening");
    frontend::run_query_frontend(cfg, catalog, readiness, &security.server, shutdown).await?;
    Ok(())
}
