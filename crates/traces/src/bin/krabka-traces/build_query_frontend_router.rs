#[cfg(test)]
use super::*;

#[cfg(test)]
pub(crate) async fn build_query_frontend_router(
    cli: &Cli,
) -> Result<axum::Router, Box<dyn std::error::Error + Send + Sync>> {
    use krabka_traces::frontend::{HttpQuerier, MembershipView, QueryFrontend};

    let addr: SocketAddr = cli.listen.parse()?;
    let cfg = frontend_config_from_cli(cli, addr)?;
    let catalog =
        build_trace_index_catalog(cli, &ServiceMetrics::new(), None, &SharedObjectStore::new())
            .await?;
    let backend = HttpQuerier::new(
        cfg.request_timeout.to_std(),
        cfg.querier_scheme,
        &InternalClient::default(),
    )?;
    // The router test double skips the probe loop: the configured endpoints
    // are the membership, all ready.
    let membership = MembershipView::fixed(cfg.querier_addrs.clone());
    let qf = Arc::new(QueryFrontend::new(
        Arc::new(backend),
        Arc::new(catalog),
        cfg,
        membership,
    ));
    Ok(
        krabka_observability::server_security::authenticate_requests(
            frontend::server::router_with_backend(qf, krabka_observability::RoleReadiness::new()),
            &ServerSecurity::default(),
        ),
    )
}
