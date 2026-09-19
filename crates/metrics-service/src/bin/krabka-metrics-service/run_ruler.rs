use krabka_blockstore::MeteredObjectStore;
use krabka_observability::{CriticalTaskError, SupervisedTasks};

use super::{
    Arc, AuditHandle, AutoOffsetReset, BrokerTransport, Cli, ClientSecurity, Consumer, LeaseConfig,
    MemberId, ObjectStore, PrometheusApiState, Role, RoleReadiness, RulerAlertmanagerSink,
    RulerShard, ServerSecurity, Shutdown, WalHead, install_bundled_rule_groups,
    load_runtime_overrides, mimir_alertmanager_router, mimir_ruler_prometheus_router,
    mimir_ruler_router, poll_ruler_state_consumer_once, query_engine_opts, readiness_router,
    run_fenced_ruler_evaluation_loop, run_ruler_state_consumer_loop,
    serve_prometheus_router_joinable, spawn_shutdown_signal_listener,
};

#[tracing::instrument(
    level = "info",
    name = "metrics.run_ruler",
    skip_all,
    fields(listen = %cli.listen, tenant = %cli.ruler_tenant, shard_index = cli.ruler_shard_index, shard_total = cli.ruler_shard_total),
    err
)]
pub(crate) async fn run_ruler(
    cli: Cli,
    metrics: krabka_promql::metrics::ServiceMetrics,
    readiness: RoleReadiness,
    security: &ServerSecurity,
    wal_security: Option<ClientSecurity>,
    audit: AuditHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let ruler_metrics = metrics.clone();
    let object_store_url = url::Url::parse(&cli.object_store_url)?;
    let (store, prefix) = object_store::parse_url_opts(&object_store_url, std::env::vars())?;
    let store: Arc<dyn ObjectStore> =
        Arc::new(object_store::prefix::PrefixStore::new(store, prefix));
    let object_store_metrics = metrics.object_store.clone();
    readiness.track_object_store(object_store_metrics.clone());
    let store = MeteredObjectStore::wrap(store, object_store_metrics);
    let config_store = Arc::clone(&store);
    let metric_store = krabka_metrics_service::RefreshingMetricBlockStore::new(
        store,
        object_store_url.clone(),
        &cli.manifest_prefix,
        WalHead::new(),
    )
    .with_cold_cache_ttl(cli.cold_cache_ttl)
    .with_unbounded_compatibility_lookback(cli.unbounded_compatibility_lookback);
    let state = PrometheusApiState::new(Arc::new(metric_store), query_engine_opts(&cli))
        .with_max_concurrent_queries(cli.max_concurrent_queries)
        .with_query_timeout(cli.query_timeout)
        .with_remote_read_max_body(cli.remote_read_max_body)
        .with_runtime_status(
            krabka_observability::LogLevelControl::process().level(),
            None,
        )
        .with_mimir_config_store(config_store)
        .with_metrics(metrics)
        .with_audit(audit);
    let state = state.with_query_limits(load_runtime_overrides(cli.runtime_overrides.as_deref())?);
    state
        .reload_mimir_configs()
        .await
        .map_err(std::io::Error::other)?;
    let state = Arc::new(state);
    let shard = RulerShard::new(cli.ruler_shard_index, cli.ruler_shard_total)?;
    let bootstrap = cli.wal_bootstrap.clone().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "--wal-bootstrap is required for --target ruler",
        )
    })?;

    // The listener binds before the bundled rules install, because they
    // install through it. These gates keep the ruler out of rotation until the
    // rules are in and both broker clients connect.
    let bundled_rules = cli
        .ruler_bundled_rules
        .as_deref()
        .map(|path| (path, readiness.gate("bundled-rules")));
    let wal_broker = readiness.gate("wal-broker");
    let ruler_state_replay = readiness.gate("ruler-state-replay");
    let router = mimir_ruler_prometheus_router(Arc::clone(&state))
        .merge(mimir_ruler_router(Arc::clone(&state)))
        .merge(mimir_alertmanager_router(Arc::clone(&state)))
        .merge(readiness_router(readiness));
    // Installed before the first broker connect, and raced against it: the
    // clients retry an unreachable bootstrap rather than reporting it, so a
    // ruler that starts against a broker that is down would otherwise sit in
    // `build` with no handler for the signal that is trying to stop it.
    let shutdown = Shutdown::new();
    spawn_shutdown_signal_listener(shutdown.clone());
    let (bound, server) =
        serve_prometheus_router_joinable(cli.listen, router, security, shutdown.signalled())
            .await?;
    tracing::info!(
        %bound,
        tls = security.tls_enabled(),
        authentication = security.authentication_enabled(),
        "metrics-service ruler listening"
    );

    let startup = async {
        // Before the ruler reaches Kafka, so a rule file an operator names but
        // the ruler cannot install stops the start early.
        if let Some((path, gate)) = bundled_rules {
            let groups =
                install_bundled_rule_groups(bound, security, path, &cli.ruler_tenant).await?;
            gate.mark_ready();
            tracing::info!(
                path = %path.display(),
                groups = groups.len(),
                "metrics ruler installed the bundled rule groups"
            );
        }
        let state_consumer = tokio::select! {
            biased;
            () = shutdown.signalled() => return Ok(None),
            built = Consumer::builder()
                .bootstrap(bootstrap.clone())
                .maybe_security(wal_security.clone())
                .dispatch_queue_capacity(cli.client_dispatch_queue_capacity)
                .frame_max(cli.client_frame_max)
                .group_id(format!(
                    "{}-ruler-state-{}-{}",
                    cli.wal_group_id,
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos()
                ))
                .client_id(format!("{}-ruler-state", cli.wal_client_id))
                .auto_offset_reset(AutoOffsetReset::Earliest)
                .subscribe([cli.ruler_state_topic.clone()])
                .build() => built?,
        };
        let coordination = tokio::select! {
            biased;
            () = shutdown.signalled() => return Ok(None),
            built = BrokerTransport::builder()
                .bootstrap(bootstrap)
                .client_id(format!("{}-ruler-coordination", cli.wal_client_id))
                .lease_duration(cli.ruler_lease_duration)
                .topic_replication(cli.ruler_coordination_replication)
                .maybe_security(wal_security)
                .build() => built?,
        };
        Ok::<_, Box<dyn std::error::Error>>(Some((state_consumer, coordination)))
    };
    let (mut state_consumer, coordination) = match startup.await {
        Ok(Some(clients)) => clients,
        Ok(None) => {
            server.await?;
            return Ok(());
        }
        Err(error) => {
            shutdown.trigger();
            if let Err(join_error) = server.await {
                tracing::warn!(%join_error, "metrics ruler server task failed while the start stopped");
            }
            return Err(error);
        }
    };
    wal_broker.mark_ready();
    loop {
        tokio::select! {
            biased;
            () = shutdown.signalled() => {
                server.await?;
                return Ok(());
            }
            result = poll_ruler_state_consumer_once(
                &mut state_consumer,
                &state,
                &cli.ruler_state_topic,
                cli.wal_poll_timeout,
            ) => result?,
        };
        if state_consumer.at_log_end().await {
            break;
        }
    }
    ruler_state_replay.mark_ready();
    let interval = cli.ruler_eval_interval;
    let state_for_replay = Arc::clone(&state);
    let state_topic = cli.ruler_state_topic.clone();
    let poll_timeout = cli.wal_poll_timeout;

    let alert_sink = ruler_alert_sink(&cli);
    let (role, member, lease_config) = ruler_coordination_identity(&cli)?;
    let coordination = Arc::new(coordination);

    // The ruler state consumer and evaluation loop are critical: both feed
    // ruler correctness, and neither loop returns voluntarily. Supervising
    // them means an exit of any kind -- an error, an early return, or a panic
    // that no `if let Err` could have seen -- ends the role by name instead of
    // leaving a ruler that serves its API and evaluates nothing.
    let mut tasks = SupervisedTasks::new(shutdown.token().clone());
    let consumer_stop = shutdown.clone();
    tasks.spawn("metrics ruler state consumer", async move {
        let result = run_ruler_state_consumer_loop(
            &mut state_consumer,
            &state_for_replay,
            &state_topic,
            poll_timeout,
            move |_| consumer_stop.is_triggered(),
        )
        .await;
        if let Err(error) = result {
            tracing::error!(%error, "metrics ruler state consumer stopped");
        }
    });
    let eval_shutdown = shutdown.clone();
    let eval_alert_sink = alert_sink.clone();
    tasks.spawn("metrics ruler evaluation", async move {
        let result = run_fenced_ruler_evaluation_loop(
            state,
            eval_alert_sink,
            coordination,
            role,
            member,
            shard,
            (cli.wal_topic.clone(), cli.ruler_state_topic.clone()),
            interval,
            lease_config,
            ruler_metrics,
            eval_shutdown.signalled(),
        )
        .await;
        if let Err(error) = result {
            tracing::error!(%error, "metrics ruler evaluation loop stopped");
        }
    });

    // Join the server task so in-flight requests drain (graceful shutdown)
    // before the process exits.
    let outcome = tokio::select! {
        result = server => result.map_err(Into::into),
        name = tasks.first_unexpected_exit() => Err(Box::<dyn std::error::Error>::from(
            CriticalTaskError(name),
        )),
    };
    shutdown.trigger();
    tasks.shutdown().await;
    let drain_result = alert_sink.shutdown().await;
    outcome?;
    drain_result?;
    Ok(())
}

fn ruler_coordination_identity(
    cli: &Cli,
) -> Result<(Role, MemberId, LeaseConfig), Box<dyn std::error::Error>> {
    let role_name = format!(
        "krabka-metrics-ruler-{}-of-{}",
        cli.ruler_shard_index, cli.ruler_shard_total
    );
    let replica = cli.ruler_replica_id.clone().unwrap_or_else(|| {
        format!(
            "{}-{}",
            std::env::var("HOSTNAME").unwrap_or_else(|_| cli.wal_client_id.clone()),
            std::process::id()
        )
    });
    Ok((
        Role::new(&role_name)?,
        MemberId::new(&replica)?,
        LeaseConfig::new(
            cli.ruler_lease_duration,
            cli.ruler_lease_renew_interval,
            cli.ruler_lease_challenge_stagger,
        )?,
    ))
}

fn ruler_alert_sink(cli: &Cli) -> RulerAlertmanagerSink {
    let external_labels = cli
        .ruler_external_label
        .iter()
        .chain(
            cli.ruler_external_label_env
                .iter()
                .flat_map(|labels| &labels.0),
        )
        .cloned()
        .collect();
    RulerAlertmanagerSink::from_endpoints(
        cli.ruler_alertmanager_url.clone(),
        external_labels,
        cli.ruler_generator_url_template.clone(),
        cli.ruler_alertmanager_queue_capacity,
    )
}
