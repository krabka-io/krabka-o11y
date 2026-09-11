use krabka_observability::RoleReadiness;
use krabka_units::{ByteSize, convert::ByteSizeExt as _};

use super::{
    BlockStoreGates, CancellationToken, Cli, ServiceMetrics, SocketAddr, build_trace_index_catalog,
    frontend, frontend_config_from_cli,
};

pub(crate) async fn run_query_frontend(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = cli.listen.parse()?;
    let cfg = frontend_config_from_cli(&cli, addr)?;
    // The catalog is built before the listener binds; `querier-membership` is
    // the gate that keeps moving after it, and `frontend::run_query_frontend`
    // registers that one.
    let gates = (cli.target_bytes_per_job > ByteSize::from_bytes(0))
        .then(|| BlockStoreGates::register(&readiness));
    let catalog = build_trace_index_catalog(&cli, &metrics, gates.as_ref()).await?;
    tracing::info!(%addr, "traces query-frontend listening");
    frontend::run_query_frontend(cfg, catalog, readiness, shutdown).await?;
    Ok(())
}
