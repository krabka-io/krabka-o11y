use std::{collections::BTreeMap, net::Ipv4Addr};

use krabka_observability::{RoleKind, RoleReadiness};

use super::{
    AllStage, Arc, CancellationToken, Cli, FrontendConfig, QuerierState, ServiceMetrics,
    SocketAddr, bind_all_stage_server, block_builder_stage, build_distributor_state,
    build_object_store, build_profile_read_path, compactor_stage, debuginfod_config,
    load_profiles_limits_overrides_config, read_path_stage, symbolizer_stage,
};

/// Builds every role `--target all` runs, and every listener it binds, before
/// a single one of them is started.
///
/// `None` is a stop, not a failure: the two builds that can block on something
/// that never answers -- the WAL producer and the block index -- are raced
/// against `shutdown`, and a process asked to stop while it was still starting
/// has nothing to drain.
///
/// The map is keyed by role rather than ordered, because the order belongs to
/// [`DRAIN_ORDER`](krabka_profiles::all::DRAIN_ORDER) and this function has no
/// business restating it.
///
/// # Errors
/// Returns an error when the object store cannot be built, when a limits file
/// cannot be read, when the block index cannot be loaded, or when either
/// listener cannot be bound.
pub(crate) async fn build_all_stages(
    cli: &Arc<Cli>,
    metrics: &ServiceMetrics,
    readiness: &RoleReadiness,
    shutdown: &CancellationToken,
) -> Result<Option<BTreeMap<RoleKind, AllStage>>, Box<dyn std::error::Error>> {
    // One object store, built once, for every role that reads or writes
    // blocks. Four roles call `build_object_store` when they run alone, and
    // four calls here would be four independent stores: with
    // `--object-store-url memory://` the block builder would write into a
    // store no reader ever opens, and the process would start, pass its
    // probes, ingest happily and answer every query with nothing.
    let configured = build_object_store(&cli.object_store_url, metrics.object_store.clone())
        .map_err(|e| format!("object store: {e}"))?;
    let index_key = configured.object_key(&cli.index_object_key);
    let store = configured.store;
    // The gate each of those roles registers for itself when it runs alone,
    // met here by the one store they now share. `/ready` still names all four,
    // because an operator reading a probe should see the same role names in
    // `all` as in the deployment it collapsed.
    for role in [
        RoleKind::BlockBuilder,
        RoleKind::Querier,
        RoleKind::QueryFrontend,
        RoleKind::Compactor,
    ] {
        readiness.for_role(role).gate("object-store").mark_ready();
    }

    let distributor = build_distributor_state(
        cli,
        metrics,
        &readiness.for_role(RoleKind::Distributor),
        shutdown,
    )
    .await?;
    let Some(distributor) = distributor else {
        return Ok(None);
    };

    let debuginfod = debuginfod_config(cli)?;
    let overrides =
        load_profiles_limits_overrides_config(cli.profiles_limits_overrides_config.as_deref())?;
    let querier_index_gate = readiness.for_role(RoleKind::Querier).gate("profile-index");
    let frontend_index_gate = readiness
        .for_role(RoleKind::QueryFrontend)
        .gate("profile-index");
    let read = build_profile_read_path(
        cli,
        Arc::clone(&store),
        index_key.clone(),
        debuginfod,
        querier_index_gate,
        shutdown,
    )
    .await?;
    let Some(read) = read else { return Ok(None) };
    // Both read roles answer from the store that has just finished loading, so
    // one snapshot meets both gates.
    frontend_index_gate.mark_ready();

    let frontend_state = Arc::new(
        QuerierState::new_frontend_with_overrides(
            Arc::clone(&read.union),
            FrontendConfig {
                shard_width: cli.query_frontend_shard_width,
            },
            overrides.clone(),
        )
        .with_heatmap_policy(cli.heatmap_value_buckets, cli.heatmap_time_buckets_max)
        .with_metrics(metrics.clone()),
    );
    let querier_state = Arc::new(
        QuerierState::new_with_overrides(Arc::clone(&read.union), overrides)
            .with_heatmap_policy(cli.heatmap_value_buckets, cli.heatmap_time_buckets_max)
            .with_metrics(metrics.clone()),
    );

    // Pyroscope serves ingest and query on one port, and so does this. The
    // distributor's routes and the query routes are disjoint -- `/ingest`,
    // `/v1development/profiles`, `push.v1.PusherService` and the OTLP profiles
    // service on one side; `/pyroscope/render`, `/pyroscope/render-diff`,
    // `querier.v1.QuerierService` and `settings.v1.SettingsService` on the
    // other -- so they merge into one router with nothing shadowed.
    //
    // The query half of that router is the query-frontend's, because the
    // frontend is Pyroscope's front door and because the two read roles serve
    // the *same* paths: whichever of them takes `--listen`, the other cannot
    // also have it.
    //
    // The listener is the distributor's stage, and so closes first. That is
    // the point of the order rather than an accident of it: shutting the
    // public door is exactly how a distributor stops writing to the WAL, and
    // the block builder behind it then drains a topic nothing is adding to.
    let door = krabka_profiles::distributor::router(distributor)
        .merge(krabka_profiles::query::router(frontend_state))
        .merge(krabka_observability::readiness_router(readiness.clone()));
    let (bound, door_stage) =
        bind_all_stage_server(cli.listen, door, "profiles all-in-one").await?;
    tracing::info!(%bound, "profiles all-in-one ingest and query listening");

    // The plain querier, on a loopback port the kernel picks. Nothing fans out
    // to it: this crate's query-frontend is a querier with a shard width, not
    // an HTTP dispatcher, so there is no set of querier addresses to wire and
    // no upstream behaviour that expects one. It binds anyway because
    // `--target all` promises to run the role, and a role that answers only
    // when it is the whole process is a role this composition never tests.
    let querier_app = krabka_profiles::query::router(querier_state)
        .merge(krabka_observability::readiness_router(readiness.clone()));
    let (loopback, loopback_stage) = bind_all_stage_server(
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        querier_app,
        "profiles querier",
    )
    .await?;
    tracing::info!(%loopback, "profiles querier listening");

    let mut stages: BTreeMap<RoleKind, AllStage> = BTreeMap::new();
    stages.insert(RoleKind::Distributor, door_stage);
    stages.insert(RoleKind::Querier, loopback_stage);
    stages.insert(
        RoleKind::BlockBuilder,
        block_builder_stage(cli, &store, &index_key, metrics),
    );
    stages.insert(RoleKind::QueryFrontend, read_path_stage(cli, read, metrics));
    stages.insert(
        RoleKind::Symbolizer,
        symbolizer_stage(cli.debuginfod_urls.clone(), debuginfod),
    );
    stages.insert(
        RoleKind::Compactor,
        compactor_stage(cli, store, index_key, metrics),
    );
    Ok(Some(stages))
}
