use super::{
    CancellationToken, Cli, ServiceMetrics, SocketAddr, build_trace_index_catalog, frontend,
    frontend_config_from_cli,
};

pub(crate) async fn run_query_frontend(
    cli: Cli,
    metrics: ServiceMetrics,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = cli.listen.parse()?;
    let cfg = frontend_config_from_cli(&cli, addr)?;
    let catalog = build_trace_index_catalog(&cli, &metrics).await?;
    tracing::info!(%addr, "traces query-frontend listening");
    frontend::run_query_frontend(cfg, catalog, shutdown).await?;
    Ok(())
}
