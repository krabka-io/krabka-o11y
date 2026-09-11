use std::collections::BTreeMap;

use krabka_observability::RoleKind;
use tokio::net::TcpListener;

use super::{
    AllRoleContext, AllRoleStage, all_role_stage, log_role_outcome, run_all_query_frontend,
    run_block_builder, run_compactor, run_distributor, run_live_store, run_metrics_generator,
    run_querier,
};

/// Every role `--target all` runs, keyed by name and not yet started.
///
/// The three listeners are already bound, and which port each one is on is
/// half of what this function decides:
///
/// * `frontend` is `--listen`. That is the port a Grafana Tempo datasource is
///   given, and the query-frontend is what answers it, because the frontend is
///   the front of the read path in every other deployment too.
/// * `querier` and `live_store` are ephemeral loopback ports. Nothing outside
///   the process has business dialling them, and pinning them to fixed numbers
///   would make two all-in-one processes on one machine collide over ports
///   neither of their operators chose. Their addresses are read back from the
///   listeners rather than guessed, and handed to the roles that dial them:
///   the frontend fans out to `querier` over real HTTP, and the querier reaches
///   `live_store` through `--querier-live-store-url`, so both of those paths
///   are the ones a distributed deployment runs rather than an in-process
///   shortcut.
///
/// # Errors
/// Returns an error when a bound listener cannot report its own address.
pub(crate) fn all_role_stages(
    ctx: &AllRoleContext,
    frontend: TcpListener,
    querier: TcpListener,
    live_store: TcpListener,
) -> Result<BTreeMap<RoleKind, AllRoleStage>, Box<dyn std::error::Error + Send + Sync>> {
    let querier_addr = querier.local_addr()?;
    let live_store_addr = live_store.local_addr()?;
    let mut stages = BTreeMap::new();

    let distributor = ctx.clone();
    stages.insert(
        RoleKind::Distributor,
        all_role_stage(move |token| async move {
            log_role_outcome(
                RoleKind::Distributor,
                run_distributor(
                    distributor.cli,
                    distributor.metrics,
                    distributor.readiness.for_role(RoleKind::Distributor),
                    token,
                    false,
                )
                .await,
            );
        }),
    );

    let builder = ctx.clone();
    stages.insert(
        RoleKind::BlockBuilder,
        all_role_stage(move |token| async move {
            log_role_outcome(
                RoleKind::BlockBuilder,
                run_block_builder(
                    builder.cli,
                    builder.metrics,
                    builder.readiness.for_role(RoleKind::BlockBuilder),
                    token,
                    &builder.object_store,
                )
                .await,
            );
        }),
    );

    let live = ctx.clone();
    stages.insert(
        RoleKind::LiveStore,
        all_role_stage(move |token| async move {
            log_role_outcome(
                RoleKind::LiveStore,
                run_live_store(
                    live.cli,
                    live.metrics,
                    live.readiness.for_role(RoleKind::LiveStore),
                    token,
                    live_store,
                )
                .await,
            );
        }),
    );

    let mut reader = ctx.clone();
    // The querier reads the recent window from the live-store role over HTTP
    // rather than from a live tier of its own. Both would answer, but only one
    // of them is the live-store actually running: a querier with
    // `--querier-live-store` set consumes the WAL itself, and `--target all`
    // would then be six roles and a copy of the seventh.
    reader.cli.querier_live_store = false;
    reader.cli.querier_live_store_url = Some(format!("http://{live_store_addr}"));
    stages.insert(
        RoleKind::Querier,
        all_role_stage(move |token| async move {
            log_role_outcome(
                RoleKind::Querier,
                run_querier(
                    reader.cli,
                    reader.metrics,
                    reader.readiness.for_role(RoleKind::Querier),
                    token,
                    querier,
                    &reader.object_store,
                )
                .await,
            );
        }),
    );

    let front = ctx.clone();
    stages.insert(
        RoleKind::QueryFrontend,
        all_role_stage(move |token| async move {
            log_role_outcome(
                RoleKind::QueryFrontend,
                run_all_query_frontend(front, frontend, querier_addr, token).await,
            );
        }),
    );

    let compactor = ctx.clone();
    stages.insert(
        RoleKind::Compactor,
        all_role_stage(move |token| async move {
            log_role_outcome(
                RoleKind::Compactor,
                run_compactor(
                    compactor.cli,
                    compactor.metrics,
                    compactor.readiness.for_role(RoleKind::Compactor),
                    token,
                    &compactor.object_store,
                )
                .await,
            );
        }),
    );

    // Tempo's `-target=all` includes the metrics-generator unconditionally.
    // This one does not, and the reason is in `MetricsGenConfig::default`: its
    // `remote_write_url` is `http://localhost:9009/api/v1/push`, Mimir's push
    // endpoint, which on the single machine this target exists for is nothing
    // at all. `MetricsGenService::run` logs `metrics-generator flush failed`
    // once per collection interval, for ever, and the first thing an operator
    // trying the stack would see is a role failing at a URL they never chose.
    // Tempo gets away with it because its `all` is a demo of the whole Grafana
    // stack, Mimir included. So: the role joins the composition when the
    // operator has said where the metrics go, by `--remote-write-url` or by a
    // `--config` file, and otherwise says once that it is not running.
    let generator = ctx.clone();
    if generator.cli.remote_write_url.is_some() || generator.cli.config.is_some() {
        stages.insert(
            RoleKind::MetricsGenerator,
            all_role_stage(move |token| async move {
                log_role_outcome(
                    RoleKind::MetricsGenerator,
                    run_metrics_generator(
                        generator.cli,
                        generator.readiness.for_role(RoleKind::MetricsGenerator),
                        token,
                    )
                    .await,
                );
            }),
        );
    } else {
        tracing::info!(
            "traces all-in-one is not running the metrics-generator: no --remote-write-url \
             and no --config, so it would have nowhere to send what it derives"
        );
    }

    Ok(stages)
}
