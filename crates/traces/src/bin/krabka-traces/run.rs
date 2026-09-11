use krabka_observability::RoleReadiness;

use super::{
    CancellationToken, Cli, OtlpConfig, ServiceMetrics, Target, require_role_topics,
    run_block_builder, run_compactor, run_distributor, run_live_store, run_metrics_generator,
    run_querier, run_query_frontend,
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
    let result = async {
        let metrics = ServiceMetrics::new();
        // The admin port binds first, before the role reaches its object store,
        // its broker, or its queriers, so `/ready` there reports the rest of the
        // start rather than "a listener exists". The roles with a Tempo data
        // port echo the same gates on it, at `/ready` and at Tempo's `/status`
        // alias, because that is the port the query-frontend probes.
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

        let role = async {
            // Before any producer or consumer exists. The traces WAL is keyed
            // by trace id, so a role started against the wrong partition count
            // would split one trace's spans across two partitions and report
            // nothing; this is where it refuses instead.
            require_role_topics(&cli).await?;
            match cli.target {
                Target::Distributor => {
                    run_distributor(cli, metrics, readiness, shutdown).await?;
                }
                Target::BlockBuilder => {
                    run_block_builder(cli, metrics, readiness, shutdown).await?;
                }
                Target::LiveStore => run_live_store(cli, metrics, readiness, shutdown).await?,
                Target::Querier => run_querier(cli, metrics, readiness, shutdown).await?,
                Target::QueryFrontend => {
                    run_query_frontend(cli, metrics, readiness, shutdown).await?;
                }
                Target::Compactor => run_compactor(cli, metrics, readiness, shutdown).await?,
                Target::MetricsGenerator => run_metrics_generator(cli, readiness, shutdown).await?,
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        };
        tokio::select! {
            result = role => result?,
            result = krabka_telemetry::profiling::await_admin_exit(admin) => result?,
        }
        Ok(())
    }
    .await;
    telemetry.shutdown();
    result
}
