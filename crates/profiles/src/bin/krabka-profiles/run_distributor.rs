use krabka_observability::{CriticalTaskError, RoleReadiness, SupervisedTasks};

use super::{
    CancellationToken, Cli, ProcessSecurity, ServiceMetrics, build_distributor_state,
    load_profiles_limits_overrides_config, serve_supervised,
};

/// Accepts pushes at the Pyroscope ingest doors and writes the profiles WAL.
///
/// `security` sets the TLS and authentication of `--listen`, and the TLS and
/// SASL of the WAL producer.
///
/// # Errors
/// Returns an error when the overrides file cannot be read or parsed, when the
/// state cannot be built, when `--listen` cannot be bound, or when the accept
/// loop ends before the role was asked to stop.
pub(crate) async fn run_distributor(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
    security: ProcessSecurity,
) -> Result<(), Box<dyn std::error::Error>> {
    let overrides =
        load_profiles_limits_overrides_config(cli.profiles_limits_overrides_config.as_deref())?;
    let Some(state) = build_distributor_state(
        &cli,
        &metrics,
        &readiness,
        &shutdown,
        overrides,
        security.wal,
    )
    .await?
    else {
        return Ok(());
    };
    let mut tasks = SupervisedTasks::new(shutdown.clone());
    let (bound, server) = serve_supervised(
        cli.listen,
        state,
        readiness,
        &security.server,
        shutdown.clone(),
    )
    .await?;
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
