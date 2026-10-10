use std::{error::Error, panic::AssertUnwindSafe, sync::Arc};

use futures::{FutureExt as _, future::FusedFuture as _};
use krabka_metrics::runtime::{WriterConfig, WriterTarget, serve_writer};
use krabka_observability::{CancellationToken, RoleKind, StagedDrain};
use prometheus_client::registry::Registry;
use tokio::sync::{Mutex, mpsc};

use super::{
    Cli, ClientSecurity, RoleAudit, RoleLaunch, RoleReadiness, ServerSecurity, Shutdown, WAL_TOPIC,
    readiness_router, run_querier, start_role_audit,
};

type RoleError = Box<dyn Error + Send + Sync>;

/// Installs both signal handlers before any role binds a listener.
pub async fn run_all(
    cli: Cli,
    security: ServerSecurity,
    wal_security: Option<ClientSecurity>,
) -> Result<(), RoleError> {
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let stopping = CancellationToken::new();
    let signal_stop = stopping.clone();
    let signal = tokio::spawn(async move {
        tokio::select! {
            _ = interrupt.recv() => {},
            _ = terminate.recv() => {},
            () = signal_stop.cancelled() => {},
        }
        signal_stop.cancel();
    });
    let outcome = serve_all(cli, security, wal_security, stopping.clone()).await;
    stopping.cancel();
    signal.await?;
    outcome
}

fn writer_config(cli: &mut Cli) -> Result<WriterConfig, RoleError> {
    let writer = WriterConfig::from_file(
        cli.writer_config
            .as_deref()
            .ok_or("--target all needs --writer-config")?,
    )?;
    if cli.listen.port() != 0
        && cli.listen.port() == writer.listen().port()
        && (cli.listen.ip() == writer.listen().ip()
            || cli.listen.ip().is_unspecified()
            || writer.listen().ip().is_unspecified())
    {
        return Err(
            "all-in-one ingest and query listeners overlap; set distinct --listen addresses".into(),
        );
    }
    if cli
        .wal_bootstrap
        .as_deref()
        .is_some_and(|broker| broker != writer.bootstrap())
    {
        return Err("all-in-one --wal-bootstrap differs from the writer bootstrap".into());
    }
    if cli.wal_group_id == writer.block_builder_group_id() {
        return Err("all-in-one builder and querier need distinct consumer groups".into());
    }
    if cli.object_store_url != writer.object_store_url() {
        return Err("all-in-one writer and querier object-store-url differ".into());
    }
    if cli.runtime_overrides.as_deref() != writer.runtime_overrides() {
        return Err("all-in-one writer and querier runtime-overrides differ".into());
    }
    if cli.wal_topic != WAL_TOPIC || cli.manifest_prefix != "metrics" {
        return Err(
            "all-in-one query roles must use the metrics WAL and manifest namespace".into(),
        );
    }
    cli.wal_bootstrap = Some(writer.bootstrap().to_owned());
    Ok(writer)
}

fn stage<F, Fut>(
    drain: &mut StagedDrain,
    errors: mpsc::UnboundedSender<String>,
    name: &'static str,
    role: F,
) where
    F: FnOnce(CancellationToken) -> Fut,
    Fut: Future<Output = Result<(), RoleError>> + Send + 'static,
{
    drain.stage(name, move |token| {
        // Construct first: the real coordinator registers startup synchronously.
        let role = role(token);
        async move {
            let failure = match AssertUnwindSafe(role).catch_unwind().await {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(format!("{name}: {error}")),
                Err(_) => Some(format!("{name}: role panicked")),
            };
            if let Some(failure) = failure {
                let _ = errors.send(failure);
            }
        }
    });
}

pub async fn serve_all(
    mut cli: Cli,
    security: ServerSecurity,
    wal_security: Option<ClientSecurity>,
    stopping: CancellationToken,
) -> Result<(), RoleError> {
    let writer = writer_config(&mut cli)?;
    let registry = Arc::new(Mutex::new(Registry::default()));
    let readiness = RoleReadiness::new();
    let serving = readiness.gate("serving");
    serving.mark_ready();
    let mut writer_metrics = Vec::new();
    for target in [
        WriterTarget::Distributor,
        WriterTarget::BlockBuilder,
        WriterTarget::Compactor,
    ] {
        writer_metrics.push((
            target,
            krabka_metrics::metrics::ServiceMetrics::for_role(Arc::clone(&registry), target.kind())
                .await,
        ));
    }
    let query_metrics =
        krabka_promql::metrics::ServiceMetrics::for_role(Arc::clone(&registry), RoleKind::Querier)
            .await;
    let RoleAudit {
        handle: audit,
        tasks: mut audit_tasks,
    } = start_role_audit(&cli, wal_security.as_ref()).await?;
    let security = security.with_security_events(Arc::new(audit.clone()));
    let admin_address = cli.admin_listen_addr;
    let profiling = cli.profiling.clone();
    let mut drain = StagedDrain::new(cli.drain_timeout);
    let (errors, mut failures) = mpsc::unbounded_channel();
    // Stop ingest first, then persist its WAL, then stop queries and compaction.
    let compactor = writer_metrics.pop().expect("three writer roles");
    for (target, metrics) in writer_metrics {
        let config = writer.clone();
        let role_readiness = readiness.for_role(target.kind());
        let role_security = security.clone();
        let wal = wal_security.clone();
        stage(
            &mut drain,
            errors.clone(),
            target.kind().as_str(),
            move |token| {
                serve_writer(
                    (target, config),
                    metrics,
                    role_readiness,
                    role_security,
                    wal,
                    token,
                )
            },
        );
    }
    let compactor_security = security.clone();
    let query_readiness = readiness.for_role(RoleKind::Querier);
    stage(&mut drain, errors.clone(), "querier", move |token| {
        run_querier(
            RoleLaunch {
                cli,
                metrics: query_metrics,
                readiness: query_readiness,
                wal_security,
                audit,
            },
            security.clone(),
            Shutdown::from(token),
        )
    });
    let role_readiness = readiness.for_role(RoleKind::Compactor);
    stage(&mut drain, errors, "compactor", move |token| {
        serve_writer(
            (compactor.0, writer),
            compactor.1,
            role_readiness,
            compactor_security,
            None,
            token,
        )
    });
    let admin = krabka_telemetry::profiling::spawn_admin_with_config(
        admin_address,
        krabka_promql::metrics::metrics_router(registry).merge(readiness_router(readiness)),
        profiling,
    )
    .await;
    let mut admin = admin.map(|task| {
        (
            task.abort_handle(),
            krabka_telemetry::profiling::await_admin_exit(task)
                .boxed()
                .fuse(),
        )
    });
    let outcome: Result<(), RoleError> = match admin.as_mut() {
        Ok((_, exit)) => {
            tokio::select! {
                () = stopping.cancelled() => Ok(()),
                name = drain.first_unexpected_exit() => Err(format!("{name} stopped unexpectedly").into()),
                name = audit_tasks.first_unexpected_exit() => Err(format!("{name} stopped unexpectedly").into()),
                result = exit => result.map_err(|error| error.to_string().into()),
            }
        }
        Err(error) => Err(error.to_string().into()),
    };
    serving.mark_unready();
    let overran = drain.drain().await;
    // Stop audit after the ordered role drain. An overrun fails the process.
    audit_tasks.shutdown().await;
    if let Ok((abort, exit)) = admin {
        abort.abort();
        if !exit.is_terminated() {
            let _ = exit.await;
        }
    }
    let mut role_failures = Vec::new();
    while let Ok(error) = failures.try_recv() {
        role_failures.push(error);
    }
    if !overran.is_empty() || !role_failures.is_empty() {
        return Err(
            format!("metrics drain failed: overran={overran:?}, errors={role_failures:?}").into(),
        );
    }
    outcome
}

#[cfg(test)]
mod tests;
