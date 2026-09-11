use std::sync::Arc;

use krabka_observability::{
    CriticalTaskError, RoleReadiness, SupervisedTasks,
    audit::{AuditService, krabka_product},
};
use tokio::net::TcpListener;

use super::{
    CancellationToken, Cli, OtlpConfig, ProcessSecurity, ServiceMetrics, SharedObjectStore,
    SocketAddr, Target, require_role_topics, run_all, run_block_builder, run_compactor,
    run_distributor, run_live_store, run_metrics_generator, run_querier, run_query_frontend,
};

pub(crate) async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let telemetry = krabka_telemetry::init(
        OtlpConfig::from_env(
            |k| std::env::var(k).ok(),
            "krabka-traces",
            env!("CARGO_PKG_VERSION"),
            "krabka-traces",
        )?,
        "krabka_traces=info,info",
        "info",
        "krabka-traces",
    )?;
    // The stack's name for this role, not clap's rendering of the variant.
    // Everything else that names a role -- a readiness gate, a manifest, the
    // other three signals' binaries -- uses `RoleKind`'s spelling, and a log
    // line an operator greps for has to match.
    tracing::info!(
        role = cli.target.kind().as_str(),
        version = env!("CARGO_PKG_VERSION"),
        "krabka-traces starting"
    );
    // Boxed: the role futures, the audit start and the select below make this
    // block large, and every caller of `run` would carry it inline.
    let result = Box::pin(async {
        // Loaded once, before any port binds or any broker is reached, so a
        // flag combination that cannot work stops the process at once. With
        // no security flag, both give the upstream default: plain listeners
        // without authentication, and a plain-text broker connection.
        let server_security = cli.server_security.load()?;
        let wal_security = cli.wal_security.load()?;
        let metrics = ServiceMetrics::new();
        // The admin port binds first, before the role reaches its object store,
        // its broker, or its queriers, so `/ready` there reports the rest of the
        // start rather than "a listener exists". The roles with a Tempo data
        // port echo the same gates on it, at `/ready` and at Tempo's `/status`
        // alias, because that is the port the query-frontend probes.
        //
        // One `RoleReadiness` per process, not per role: under `--target all`
        // the seven roles take `for_role` views of this one list, so `/ready`
        // stays a single answer and names the role whose gate is holding the
        // process back.
        let readiness = RoleReadiness::new();
        let admin = krabka_telemetry::profiling::spawn_admin_with_config(
            cli.admin_listen_addr,
            krabka_traces::metrics::metrics_router(metrics.registry.clone())
                .merge(krabka_observability::readiness_router(readiness.clone())),
            cli.profiling.clone(),
        )
        .await?;

        let shutdown = CancellationToken::new();
        let shutdown_task = shutdown.clone();
        tokio::spawn(async move {
            krabka_observability::shutdown_signal().await;
            shutdown_task.cancel();
        });

        // Traces has no destructive admin operation, so its audit trail is
        // what the listeners report: each failed authentication and each
        // tenant denial. With no `--audit-topic` the handle discards every
        // event and no writer runs.
        let audit_shutdown = CancellationToken::new();
        let (audit, audit_writer) = AuditService::start(
            &cli.audit,
            krabka_product("krabka-traces", env!("CARGO_PKG_VERSION")),
            Some(&cli.bootstrap),
            wal_security.as_ref(),
            audit_shutdown.clone(),
        )
        .await?
        .into_parts();
        let mut audit_tasks = SupervisedTasks::new(audit_shutdown);
        if let Some(writer) = audit_writer {
            audit_tasks.adopt("traces audit writer", writer);
        }
        let security = ProcessSecurity {
            server: server_security.with_security_events(Arc::new(audit)),
            wal: wal_security,
        };

        let role = async {
            // Before any producer or consumer exists, and once for the whole
            // process however many roles it runs. The traces WAL is keyed by
            // trace id, so a role started against the wrong partition count
            // would split one trace's spans across two partitions and report
            // nothing; this is where it refuses instead.
            require_role_topics(&cli, security.wal.as_ref()).await?;
            // One object store per process, for the same reason. Built on
            // first use, so a role that never reads blocks never builds one.
            let object_store = SharedObjectStore::new();
            match cli.target {
                Target::Distributor => {
                    run_distributor(cli, metrics, readiness, shutdown, true, &security).await?;
                }
                Target::BlockBuilder => {
                    run_block_builder(cli, metrics, readiness, shutdown, &object_store, &security)
                        .await?;
                }
                Target::LiveStore => {
                    let listener = TcpListener::bind(cli.listen.parse::<SocketAddr>()?).await?;
                    run_live_store(cli, metrics, readiness, shutdown, listener, &security).await?;
                }
                Target::Querier => {
                    let listener = TcpListener::bind(cli.listen.parse::<SocketAddr>()?).await?;
                    run_querier(
                        cli,
                        metrics,
                        readiness,
                        shutdown,
                        listener,
                        &object_store,
                        &security,
                    )
                    .await?;
                }
                Target::QueryFrontend => {
                    run_query_frontend(
                        cli,
                        metrics,
                        readiness,
                        shutdown,
                        &object_store,
                        &security,
                    )
                    .await?;
                }
                Target::Compactor => {
                    run_compactor(cli, metrics, readiness, shutdown, &object_store).await?;
                }
                Target::MetricsGenerator => {
                    run_metrics_generator(cli, readiness, shutdown, &security).await?;
                }
                Target::All => {
                    run_all(cli, metrics, readiness, shutdown, security.clone()).await?;
                }
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        };
        let outcome = tokio::select! {
            result = role => result,
            result = krabka_telemetry::profiling::await_admin_exit(admin) => result.map_err(Into::into),
            name = audit_tasks.first_unexpected_exit() => Err(
                Box::<dyn std::error::Error + Send + Sync>::from(CriticalTaskError(name)),
            ),
        };
        // The role has returned, so its listeners have stopped and no request
        // can report an event after the audit queue closes. The writer then
        // writes what is in the queue and stops.
        audit_tasks.shutdown().await;
        outcome
    })
    .await;
    telemetry.shutdown();
    result
}
