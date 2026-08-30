use super::*;

/// Boot the query-frontend role.
///
/// This builds the HTTP querier pool and a block catalog, then serves the
/// router on `cfg.listen_addr` until `shutdown` fires.
///
/// `catalog` is the production [`TraceIndexCatalog`], or any compatible block
/// catalog.
///
/// # Errors
/// Propagates bind and serve `std::io` errors, and backend-construction
/// failures.
pub async fn run_query_frontend(
    cfg: FrontendConfig,
    catalog: TraceIndexCatalog,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let backend = HttpQuerier::new(cfg.querier_addrs.clone(), cfg.request_timeout.to_std())
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let listen_addr = cfg.listen_addr;
    let qf = Arc::new(crate::frontend::QueryFrontend::new(
        Arc::new(backend),
        Arc::new(catalog),
        cfg,
    ));
    let app = crate::frontend::server::router_with_backend(qf);
    let listener = tokio::net::TcpListener::bind(listen_addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
}
