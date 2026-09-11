use krabka_observability::{CriticalTaskError, SupervisedTasks};

use super::*;

/// Boot the query-frontend role.
///
/// This builds the querier transport, resolves and probes the querier pool
/// once so the first query sees real membership rather than an empty one, then
/// serves the router on `cfg.listen_addr` until `shutdown` fires.
///
/// The membership refresh runs as a supervised task, not a bare `tokio::spawn`.
/// If it stopped unnoticed the frontend would keep fanning out over whichever
/// pool it last saw -- stale, and silently so, which is the failure the loop
/// exists to prevent.
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
    let backend = HttpQuerier::new(cfg.request_timeout.to_std())
        .map_err(|e| std::io::Error::other(e.to_string()))?;
    let probe: Arc<dyn crate::frontend::membership::ReadinessProbe> = Arc::new(
        HttpReadinessProbe::new(cfg.readiness_timeout.to_std())
            .map_err(|e| std::io::Error::other(e.to_string()))?,
    );
    let membership = MembershipView::empty();
    membership.publish(refresh_membership(&cfg.querier_addrs, probe.as_ref()).await);

    let listen_addr = cfg.listen_addr;
    let endpoints = cfg.querier_addrs.clone();
    let refresh_interval = cfg.membership_refresh_interval.to_std();
    let qf = Arc::new(crate::frontend::QueryFrontend::new(
        Arc::new(backend),
        Arc::new(catalog),
        cfg,
        membership.clone(),
    ));
    let app = crate::frontend::server::router_with_backend(qf);
    let listener = tokio::net::TcpListener::bind(listen_addr).await?;

    let mut tasks = SupervisedTasks::new(shutdown.clone());
    let refresh_shutdown = shutdown.clone();
    tasks.spawn(
        "traces query-frontend membership refresh",
        run_membership_refresh(
            membership,
            endpoints,
            probe,
            refresh_interval,
            refresh_shutdown,
        ),
    );

    let server_shutdown = shutdown.clone();
    let server = axum::serve(listener, krabka_observability::contain_handler_panics(app))
        .with_graceful_shutdown(async move { server_shutdown.cancelled().await });
    let outcome = tokio::select! {
        result = server => result,
        name = tasks.first_unexpected_exit() => Err(std::io::Error::other(CriticalTaskError(name))),
    };
    tasks.shutdown().await;
    outcome
}
