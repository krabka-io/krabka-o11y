use krabka_observability::{CriticalTaskError, RoleReadiness, StagedDrain};
use krabka_traces::all_in_one::DRAIN_ORDER;
use tokio::net::TcpListener;

use super::{
    AllRoleContext, CancellationToken, Cli, ProcessSecurity, ServiceMetrics, SharedObjectStore,
    SocketAddr, all_role_stages, require_internal_credential,
};

/// The loopback address the two internal roles bind, with the port left to the
/// kernel.
///
/// Loopback because nothing outside the process may dial the querier or the
/// live-store directly: `--listen` is the process's Tempo API and the
/// query-frontend is what serves it. Port zero because a fixed internal port
/// is a collision between two all-in-one processes on one machine, over a
/// number neither operator chose or can see.
const INTERNAL_LISTEN: &str = "127.0.0.1:0";

/// Runs every traces role in one process.
///
/// The composition is [`all_role_stages`]; the order it is taken apart in is
/// [`DRAIN_ORDER`], and this function is where the two meet. Stages are
/// registered by walking `DRAIN_ORDER`, not in the order the composition
/// happens to build them, because [`StagedDrain`] stops stages in registration
/// order and that order is the difference between a block builder that drains
/// the WAL the distributor just stopped filling and one that is cancelled
/// beside it.
///
/// While the process serves, the same [`StagedDrain`] is its supervisor. A
/// role that returns on its own has left the process one role short with every
/// port still open -- a querier whose index refresher died still answers, from
/// an index that stopped moving -- so the first such exit ends the process
/// with [`CriticalTaskError`] and a non-zero status. The ordered drain still
/// runs afterwards, because the remaining roles still have the same things to
/// lose.
///
/// # Internal traffic
///
/// Every listener of the process serves with the same security: the Tempo
/// API, the ingest ports, and the two loopback ports. A loopback port is not a
/// trust boundary, because any process on the machine can dial it, so it gets
/// no exception.
///
/// - With TLS on, the query-frontend dials the querier, and the querier dials
///   the live-store, at `https://127.0.0.1`. The server certificate needs
///   `127.0.0.1` in its subject alternative names, and the internal client CA
///   bundle needs the CA that signed it.
/// - With authentication on, both callers present the internal client
///   credential. The frontend and the querier have already checked the end
///   user against the tenant, so the internal principal should hold every
///   tenant. Without an internal client credential, the process refuses to
///   start, because every query would fail at the loopback hop.
///
/// # Errors
/// Returns an error when authentication is on and no internal client is
/// configured, when a port cannot be bound, when the composition disagrees
/// with [`DRAIN_ORDER`], or when a role stops while the process was still
/// serving.
pub(crate) async fn run_all(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
    security: ProcessSecurity,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    require_internal_credential(&security.server)?;
    // Bound here, before any role starts, because the addresses are what wire
    // the roles to each other: the frontend needs the querier's port and the
    // querier needs the live-store's. Reading `local_addr` off a listener that
    // exists is the only way to learn what a port zero turned into; asking the
    // kernel for a free port and then binding it later is a race with every
    // other process on the machine.
    let frontend = TcpListener::bind(cli.listen.parse::<SocketAddr>()?).await?;
    let querier = TcpListener::bind(INTERNAL_LISTEN).await?;
    let live_store = TcpListener::bind(INTERNAL_LISTEN).await?;
    tracing::info!(
        tempo_api = %frontend.local_addr()?,
        querier = %querier.local_addr()?,
        live_store = %live_store.local_addr()?,
        "traces all-in-one starting every role in one process"
    );

    let stage_timeout = cli.all_drain_stage_timeout;
    let ctx = AllRoleContext {
        cli,
        metrics,
        readiness,
        object_store: SharedObjectStore::new(),
        security,
    };
    let mut roles = all_role_stages(&ctx, frontend, querier, live_store)?;

    let mut drain = StagedDrain::new(stage_timeout);
    for kind in DRAIN_ORDER {
        if let Some(role) = roles.remove(&kind) {
            drain.stage(kind.as_str(), role);
        }
    }
    // A role the composition builds and `DRAIN_ORDER` does not name would
    // never be started here and never be stopped. That is a role missing from
    // a list, which is exactly the kind of omission that otherwise shows up as
    // a port nobody is listening on months later, so it refuses to start.
    if let Some((kind, _)) = roles.pop_first() {
        return Err(format!(
            "--target all composes the {kind} role, which its drain order does not name"
        )
        .into());
    }

    let outcome = tokio::select! {
        () = shutdown.cancelled() => Ok(()),
        name = drain.first_unexpected_exit() => Err(
            Box::<dyn std::error::Error + Send + Sync>::from(CriticalTaskError(name)),
        ),
    };
    for stage in drain.drain().await {
        tracing::warn!(stage, "traces role did not finish draining in time");
    }
    outcome
}
