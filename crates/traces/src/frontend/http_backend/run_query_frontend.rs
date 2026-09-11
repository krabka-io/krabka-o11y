use std::future::IntoFuture as _;

use krabka_observability::{CriticalTaskError, SupervisedTasks};

use super::*;

/// Boot the query-frontend role.
///
/// This builds the querier transport, resolves and probes the querier pool
/// once so the first query sees real membership rather than an empty one, then
/// serves the router on `cfg.listen_addr` until `shutdown` fires.
///
/// `security` decides how the listener serves, and it gives the internal
/// client that the transport and the probe dial the queriers with. Every
/// route checks the request's principal against the tenant before it plans a
/// job.
///
/// The membership refresh runs as a supervised task, not a bare `tokio::spawn`.
/// If it stopped unnoticed the frontend would keep fanning out over whichever
/// pool it last saw. That is stale, and silently so, which is the failure the
/// loop exists to prevent.
///
/// `catalog` is the production [`TraceIndexCatalog`], or any compatible block
/// catalog.
///
/// `readiness` already carries the gates the caller cleared before this point.
/// This function adds `querier-membership`, which the probe loop moves up and
/// down for the life of the role. It is the one gate of a query-frontend that
/// no amount of startup can settle, because it reports other processes.
///
/// # Errors
/// Propagates bind and serve `std::io` errors, and backend-construction
/// failures.
pub async fn run_query_frontend(
    cfg: FrontendConfig,
    catalog: TraceIndexCatalog,
    readiness: RoleReadiness,
    security: &ServerSecurity,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    let internal_client = security.internal_client();
    let backend = HttpQuerier::new(
        cfg.request_timeout.to_std(),
        cfg.querier_scheme,
        internal_client,
    )
    .map_err(|e| std::io::Error::other(e.to_string()))?;
    let probe: Arc<dyn crate::frontend::membership::ReadinessProbe> = Arc::new(
        HttpReadinessProbe::new(
            cfg.readiness_timeout.to_std(),
            cfg.querier_scheme,
            internal_client,
        )
        .map_err(|e| std::io::Error::other(e.to_string()))?,
    );
    let membership_gate = readiness.gate(QUERIER_MEMBERSHIP_GATE);
    let membership = MembershipView::empty();
    let members = refresh_membership(&cfg.querier_addrs, probe.as_ref()).await;
    mark_querier_membership_gate(&membership_gate, &members);
    membership.publish(members);

    let listen_addr = cfg.listen_addr;
    let endpoints = cfg.querier_addrs.clone();
    let refresh_interval = cfg.membership_refresh_interval.to_std();
    let qf = Arc::new(crate::frontend::QueryFrontend::new(
        Arc::new(backend),
        Arc::new(catalog),
        cfg,
        membership.clone(),
    ));
    let app = crate::frontend::server::router_with_backend(qf, readiness);
    let tcp = tokio::net::TcpListener::bind(listen_addr).await?;
    let listener = ServerListener::bind(tcp, security).map_err(std::io::Error::other)?;

    let mut tasks = SupervisedTasks::new(shutdown.clone());
    let refresh_shutdown = shutdown.clone();
    tasks.spawn(
        "traces query-frontend membership refresh",
        run_membership_refresh(
            membership,
            endpoints,
            probe,
            refresh_interval,
            membership_gate,
            refresh_shutdown,
        ),
    );

    let server = serve_router(listener, app, security)
        .with_graceful_shutdown(shutdown.clone().cancelled_owned())
        .into_future();
    let outcome = tokio::select! {
        result = server => result,
        name = tasks.first_unexpected_exit() => Err(std::io::Error::other(CriticalTaskError(name))),
    };
    tasks.shutdown().await;
    outcome
}
