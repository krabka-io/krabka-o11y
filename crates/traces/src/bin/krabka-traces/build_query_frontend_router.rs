#[cfg(test)]
use super::*;

#[cfg(test)]
pub(crate) async fn build_query_frontend_router(
    cli: &Cli,
) -> Result<axum::Router, Box<dyn std::error::Error + Send + Sync>> {
    use krabka_traces::frontend::{HttpQuerier, MembershipView};

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
    Ok(
        krabka_observability::server_security::authenticate_requests(
            build_query_frontend_app::build_query_frontend_app(
                backend,
                catalog,
                cfg,
                membership,
                krabka_observability::RoleReadiness::new(),
            ),
            &ServerSecurity::default(),
        ),
    )
}
