use krabka_observability::{CriticalTaskError, RoleReadiness, StagedDrain};
use krabka_profiles::all::DRAIN_ORDER;
use krabka_units::fmt::Human as _;

use super::{Arc, CancellationToken, Cli, ProcessSecurity, ServiceMetrics, build_all_stages};

/// Runs every profiles role in one process.
///
/// This is Pyroscope's `all`, and it is what a laptop, a demo and a
/// one-container deployment run. Collapsing six roles into one process is
/// mostly a matter of not accidentally building six of everything:
///
/// * one object store, so the blocks the builder writes are the blocks the
///   read path reads;
/// * one WAL topic contract check, asked before any role opens a client;
/// * one [`RoleReadiness`], so `/ready` is a single answer for the process and
///   names the role whose gate is holding it back -- `not ready:
///   block-builder/object-store` rather than a bare `object-store` that four
///   of the roles could have registered;
/// * one Pyroscope-facing port, carrying the ingest doors and the query
///   surface together;
/// * one [`ProcessSecurity`], so both listeners and every broker connection
///   use the same TLS, credentials and audit trail.
///
/// The stop is ordered rather than simultaneous, and
/// [`DRAIN_ORDER`] is where that order is written down. A role that stops on
/// its own while the process is still serving is a fault, not a stop: the
/// process reports it as a [`CriticalTaskError`] and exits non-zero, and then
/// still walks the drain, because the records already in the WAL do not stop
/// mattering because something else broke.
///
/// # Errors
/// Returns an error when a role cannot be built or a listener cannot be bound,
/// and when a role stops on its own while the process is serving.
pub(crate) async fn run_all(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
    security: ProcessSecurity,
) -> Result<(), Box<dyn std::error::Error>> {
    let cli = Arc::new(cli);
    let stage_timeout = cli.all_drain_stage_timeout;
    let Some(mut stages) =
        build_all_stages(&cli, &metrics, &readiness, &shutdown, &security).await?
    else {
        return Ok(());
    };
    let mut drain = StagedDrain::new(stage_timeout);
    for role in DRAIN_ORDER {
        let stage = stages
            .remove(&role)
            .ok_or_else(|| format!("`--target all` builds no {role} stage"))?;
        drain.stage(role.as_str(), stage);
    }

    let outcome = tokio::select! {
        () = shutdown.cancelled() => Ok(()),
        name = drain.first_unexpected_exit() => {
            Err(Box::<dyn std::error::Error>::from(CriticalTaskError(name)))
        }
    };
    let overran = drain.drain().await;
    if !overran.is_empty() {
        tracing::warn!(
            stages = ?overran,
            per_stage_timeout = %stage_timeout.human(),
            "profiles all-in-one stopped without waiting for these roles"
        );
    }
    outcome
}
