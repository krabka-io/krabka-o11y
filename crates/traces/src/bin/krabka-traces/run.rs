use super::{CancellationToken, Cli, OtlpConfig, ServiceMetrics, Target, run_block_builder, run_compactor, run_distributor, run_live_store, run_metrics_generator, run_querier, run_query_frontend};

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
        let admin = krabka_telemetry::profiling::spawn_admin_with_config(
            cli.admin_listen_addr,
            krabka_traces::metrics::metrics_router(metrics.registry.clone()),
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
            match cli.target {
                Target::Distributor => run_distributor(cli, metrics, shutdown).await?,
                Target::BlockBuilder => run_block_builder(cli, metrics, shutdown).await?,
                Target::LiveStore => run_live_store(cli, shutdown).await?,
                Target::Querier => run_querier(cli, metrics, shutdown).await?,
                Target::QueryFrontend => run_query_frontend(cli, shutdown).await?,
                Target::Compactor => run_compactor(cli, shutdown).await?,
                Target::MetricsGenerator => run_metrics_generator(cli, shutdown).await?,
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
