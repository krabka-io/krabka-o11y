use krabka_observability::{CriticalTaskError, RoleReadiness, SupervisedTasks};

use super::{CancellationToken, Cli, ServiceMetrics, build_distributor_state, serve_supervised};

/// Accepts pushes at the Pyroscope ingest doors and writes the profiles WAL.
///
/// # Errors
/// Returns an error when the state cannot be built, when `--listen` cannot be
/// bound, or when the accept loop ends before the role was asked to stop.
pub(crate) async fn run_distributor(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(state) = build_distributor_state(&cli, &metrics, &readiness, &shutdown).await? else {
        return Ok(());
    };
    let mut tasks = SupervisedTasks::new(shutdown.clone());
    let (bound, server) = serve_supervised(cli.listen, state, readiness, shutdown.clone()).await?;
    tasks.adopt("profiles distributor HTTP", server);
    tracing::info!(%bound, "profiles distributor listening");
    let outcome = tokio::select! {
        () = shutdown.cancelled() => Ok(()),
        name = tasks.first_unexpected_exit() => {
            Err(Box::<dyn std::error::Error>::from(CriticalTaskError(name)))
        }
    };
    tasks.shutdown().await;
    outcome
}
