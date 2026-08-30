use super::*;

#[cfg(test)]
pub(crate) async fn build_query_frontend_router(
    cli: &Cli,
) -> Result<axum::Router, Box<dyn std::error::Error + Send + Sync>> {
    use krabka_traces::frontend::{HttpQuerier, QueryFrontend};

    let addr: SocketAddr = cli.listen.parse()?;
    let cfg = frontend_config_from_cli(cli, addr)?;
    let catalog = build_trace_index_catalog(cli).await?;
    let backend = HttpQuerier::new(cfg.querier_addrs.clone(), cfg.request_timeout.to_std())?;
    let qf = Arc::new(QueryFrontend::new(
        Arc::new(backend),
        Arc::new(catalog),
        cfg,
    ));
    Ok(frontend::server::router_with_backend(qf))
}
