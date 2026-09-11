use krabka_observability::RoleReadiness;

use super::{
    Cli, ServiceMetrics, Target, require_role_topics, role_shutdown_token, run_all,
    run_block_builder, run_compactor, run_distributor, run_querier, run_query_frontend,
    run_symbolizer,
};

pub(crate) async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let metrics = ServiceMetrics::new();
    // The admin port binds first, before the role reaches its object store or
    // its broker, so `/ready` there reports the rest of the start rather than
    // "a listener exists". The roles with a data port echo the same gates on
    // it.
    let readiness = RoleReadiness::new();
    let admin = krabka_telemetry::profiling::spawn_admin_with_config(
        cli.admin_listen_addr,
        krabka_profiles::metrics::metrics_router(metrics.registry.clone())
            .merge(krabka_observability::readiness_router(readiness.clone())),
        cli.profiling.clone(),
    )
    .await?;
    // The role, spelled as the shared vocabulary spells it, so that a log line
    // and a deployment manifest name the same stage.
    tracing::info!(
        role = %cli.target.kind(),
        version = env!("CARGO_PKG_VERSION"),
        "krabka-profiles starting"
    );
    // Before any role reaches a broker or an object store: until this token
    // exists no `SIGTERM` handler is installed, and a role blocked on a
    // bootstrap address that never answers would have nothing to hear the
    // signal with.
    let shutdown = role_shutdown_token();

    let role = async move {
        // Before any producer or consumer exists. A WAL topic's partition
        // count is the write-path shard count, so a role that started against
        // the wrong one would re-map every key it routes and report nothing;
        // this is where it refuses instead.
        require_role_topics(&cli).await?;
        match cli.target {
            Target::Distributor => run_distributor(cli, metrics, readiness, shutdown).await?,
            Target::BlockBuilder => run_block_builder(cli, metrics, readiness, shutdown).await?,
            Target::Querier => run_querier(cli, metrics, readiness, shutdown).await?,
            Target::QueryFrontend => run_query_frontend(cli, metrics, readiness, shutdown).await?,
            Target::Compactor => run_compactor(cli, metrics, readiness, shutdown).await?,
            Target::Symbolizer => run_symbolizer(cli).await?,
            Target::All => run_all(cli, metrics, readiness, shutdown).await?,
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    };

    tokio::select! {
        result = role => result,
        result = krabka_telemetry::profiling::await_admin_exit(admin) => Ok(result?),
    }
}
