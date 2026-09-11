use std::future::IntoFuture as _;

use krabka_observability::{
    RoleKind,
    server_security::{ServerListener, serve_router},
};
use krabka_traces::frontend::{HttpQuerier, MembershipView, QuerierScheme, QueryFrontend};
use krabka_units::{
    ByteSize,
    convert::{ByteSizeExt as _, TimeExt as _},
};
use tokio::net::TcpListener;

use super::{
    AllRoleContext, Arc, BlockStoreGates, CancellationToken, SocketAddr, build_trace_index_catalog,
    frontend, frontend_config_from_cli,
};

/// The query-frontend of `--target all`, fanning out to the querier beside it.
///
/// This is the one role the composition cannot reuse as-is, and the reason is
/// a cycle. [`frontend::run_query_frontend`] discovers its queriers by probing
/// their `/ready`, and registers a `querier-membership` gate that it holds
/// down until one of them answers 200. In a single-process stack that gate is
/// in the *same* [`RoleReadiness`] the querier's own data port reports, so the
/// querier answers 503 naming the frontend's gate, the frontend reads the 503
/// as a querier that is not ready, and neither ever moves. The process would
/// start, bind every port, and fail every query with "no querier is ready",
/// for ever.
///
/// So membership here is [`MembershipView::fixed`] over the one address the
/// composition already knows -- the port the querier's listener actually bound
/// -- and no probe loop runs. Nothing is lost by that: a probe exists to
/// notice queriers appearing and disappearing, and in this process there is
/// exactly one, it is in the same address space, and it cannot outlive the
/// frontend or be replaced under it. What is deliberately *not* skipped is the
/// fan-out itself: [`HttpQuerier`] dials `querier_addr` over real HTTP and
/// speaks the same job protocol it would to a querier in another pod, so the
/// path this target exercises is the deployed one.
///
/// The querier's loopback port serves with the process security, so the
/// fan-out dials it over TLS when the listeners serve TLS, and presents the
/// internal client credential.
///
/// [`RoleReadiness`]: krabka_observability::RoleReadiness
///
/// # Errors
/// Returns an error when the block catalog cannot be built, when the querier
/// transport cannot be constructed, or when the HTTP server fails.
pub(crate) async fn run_all_query_frontend(
    ctx: AllRoleContext,
    listener: TcpListener,
    querier_addr: SocketAddr,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let readiness = ctx.readiness.for_role(RoleKind::QueryFrontend);
    let gates = (ctx.cli.target_bytes_per_job > ByteSize::from_bytes(0))
        .then(|| BlockStoreGates::register(&readiness));
    let security = &ctx.security.server;
    let mut cfg = frontend_config_from_cli(&ctx.cli, listener.local_addr()?)?;
    // `--querier-url` names queriers in other processes. There are none: the
    // only querier this frontend may fan out to is the one in this process,
    // on the port it bound a moment ago.
    cfg.querier_addrs = vec![querier_addr.to_string()];
    cfg.querier_scheme = QuerierScheme::serving_tls(security.tls_enabled());
    let catalog =
        build_trace_index_catalog(&ctx.cli, &ctx.metrics, gates.as_ref(), &ctx.object_store)
            .await?;
    let backend = HttpQuerier::new(
        cfg.request_timeout.to_std(),
        cfg.querier_scheme,
        security.internal_client(),
    )?;
    let membership = MembershipView::fixed(cfg.querier_addrs.clone());
    let qf = Arc::new(QueryFrontend::new(
        Arc::new(backend),
        Arc::new(catalog),
        cfg,
        membership,
    ));
    let app = frontend::server::router_with_backend(qf, readiness);
    let listener = ServerListener::bind(listener, security)?;
    let bound = listener.local_addr();
    tracing::info!(%bound, %querier_addr, "traces all-in-one Tempo API listening");
    serve_router(listener, app, security)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .into_future()
        .await?;
    Ok(())
}
