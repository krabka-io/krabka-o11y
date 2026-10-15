use krabka_client_consumer::IsolationLevel;

use super::{
    Arc, AutoOffsetReset, BrokerTransport, Cli, Consumer, CriticalTaskError, Future, LeaseConfig,
    MemberId, MimirTenantAdminState, QueryFrontendOptions, ReadinessGate, Role, RoleLaunch,
    RoleObjectStore, RulerAlertmanagerSink, RulerShard, RulerTopics, ServerSecurity,
    ServingMetricStore, ServingWalHead, Shutdown, SupervisedTasks, TimeExt, WalHead,
    install_bundled_rule_groups, load_runtime_overrides, mimir_alertmanager_router,
    mimir_ruler_prometheus_router, mimir_ruler_router, mimir_tenant_admin_router,
    poll_ruler_state_consumer_once, prometheus_router, readiness_router,
    run_fenced_ruler_evaluation_loop, run_ruler_state_consumer_loop,
    serve_prometheus_router_joinable, serving_api_state, spawn_role_wal_head_consumer,
    spawn_shutdown_signal_listener,
};

#[tracing::instrument(
    level = "info",
    name = "metrics.run_querier",
    skip_all,
    fields(listen = %launch.cli.listen, object_store = %launch.cli.object_store_url, manifest_prefix = %launch.cli.manifest_prefix, wal_topic = %launch.cli.wal_topic),
)]
pub(crate) fn run_querier(
    launch: RoleLaunch,
    security: ServerSecurity,
    shutdown: Shutdown,
) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'static {
    let RoleLaunch {
        cli,
        metrics,
        readiness,
        wal_security,
        audit,
    } = launch;
    let startup = readiness.gate("startup");
    Box::pin(tracing::Instrument::instrument(
        async move {
            let role_store = RoleObjectStore::open(&cli, &metrics, &readiness)?;
            let store = Arc::clone(&role_store.store);
            let mut wal_head = ServingWalHead::open(&cli, &metrics, &readiness);
            let query_limits = load_runtime_overrides(cli.runtime_overrides.as_deref())?;
            // The WAL head consumer is the querier's recent window. If it stops, the
            // role keeps its listener and answers from the last compacted block with
            // nothing in the response to say the rest is missing, so its exit -- panic
            // included -- has to end the role.
            let mut tasks = SupervisedTasks::new(shutdown.token().clone());
            if let Some(feed) = wal_head.take_feed(wal_security, cli.wal_group_id.clone()) {
                tasks.adopt(
                    "metrics querier WAL head",
                    spawn_role_wal_head_consumer(&cli, feed, shutdown.clone()),
                );
            }
            let ServingMetricStore {
                head,
                status_wal,
                metric_store,
            } = ServingMetricStore::from_head(&role_store, &cli, wal_head);
            let state = serving_api_state(Arc::clone(&metric_store), &cli)
                .with_erasure_store(Arc::clone(&store))
                .with_metrics(metrics)
                .with_audit(audit);
            let state = if let Some(status) = status_wal {
                state.with_wal_head_status(status.head, status.readiness)
            } else {
                state
            };
            let state = state.with_query_limits(query_limits);
            let router = prometheus_router(Arc::new(state))
                .merge(mimir_tenant_admin_router(MimirTenantAdminState::new(
                    store,
                    metric_store,
                    head,
                )))
                .merge(readiness_router(readiness));
            let mut serving = match serve_prometheus_router_joinable(
                cli.listen,
                router,
                &security,
                shutdown.signalled(),
            )
            .await
            {
                Ok(server) => server,
                Err(error) => {
                    tasks.shutdown().await;
                    return Err(Box::<dyn std::error::Error + Send + Sync>::from(error));
                }
            };
            startup.mark_ready();
            tracing::info!(
                %serving.addr,
                tls = security.tls_enabled(),
                authentication = security.authentication_enabled(),
                "metrics-service querier listening"
            );
            // Join the server task so in-flight requests drain (graceful shutdown)
            // before the process exits -- unless the WAL head consumer stops first, in
            // which case the role fails by name and the drain happens on the way out.
            let outcome = tokio::select! {
                result = &mut serving.server => result.map_err(Into::into),
                name = tasks.first_unexpected_exit() => {
                    shutdown.trigger();
                    let _ = serving.server.await;
                    Err(Box::<dyn std::error::Error + Send + Sync>::from(CriticalTaskError(name)))
                },
            };
            shutdown.trigger();
            tasks.shutdown().await;
            outcome
        },
        tracing::Span::current(),
    ))
}

#[tracing::instrument(
    level = "info",
    name = "metrics.run_query_frontend",
    skip_all,
    fields(listen = %launch.cli.listen, object_store = %launch.cli.object_store_url, manifest_prefix = %launch.cli.manifest_prefix),
    err
)]
pub(crate) async fn run_query_frontend(
    launch: RoleLaunch,
    security: &ServerSecurity,
) -> Result<(), Box<dyn std::error::Error>> {
    let RoleLaunch {
        cli,
        metrics,
        readiness,
        wal_security,
        audit,
    } = launch;
    let role_store = RoleObjectStore::open(&cli, &metrics, &readiness)
        .map_err(|error| -> Box<dyn std::error::Error> { error })?;
    let store = Arc::clone(&role_store.store);
    let mut wal_head = ServingWalHead::open(&cli, &metrics, &readiness);
    let shutdown = Shutdown::new();
    spawn_shutdown_signal_listener(shutdown.clone());
    let mut tasks = SupervisedTasks::new(shutdown.token().clone());
    // Every frontend needs the complete recent window. A shared consumer group
    // would split partitions between replicas and make the public Service
    // return different answers depending on which pod it chose.
    let group_id = format!(
        "{}-{}-{}",
        cli.wal_group_id,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    if let Some(feed) = wal_head.take_feed(wal_security, group_id) {
        tasks.adopt(
            "metrics query-frontend WAL head",
            spawn_role_wal_head_consumer(&cli, feed, shutdown.clone()),
        );
    }
    let ServingMetricStore {
        head,
        status_wal,
        metric_store,
    } = ServingMetricStore::from_head(&role_store, &cli, wal_head);
    let query_cache = krabka_promql::ObjectStoreQueryFrontendCache::new(
        Arc::clone(&store),
        cli.query_frontend_cache_prefix.clone(),
    )
    .with_ttl(cli.query_frontend_cache_ttl)
    .with_execution_options(krabka_query_frontend::ExecutionOptions {
        max_parallelism: std::num::NonZeroUsize::new(cli.query_frontend_max_parallelism)
            .expect("clap rejects zero query-frontend parallelism"),
        max_retries: cli.query_frontend_max_retries,
        max_cache_freshness: cli.query_frontend_max_cache_freshness.to_std(),
    });
    {
        let mut registry = metrics.registry.lock().await;
        query_cache.metrics().register(&mut registry);
    }
    krabka_query_frontend::QueryCache::sweep(&query_cache).await?;
    let state = serving_api_state(Arc::clone(&metric_store), &cli)
        .with_erasure_store(Arc::clone(&store))
        .with_metrics(metrics)
        .with_audit(audit)
        .with_query_frontend_cache(
            QueryFrontendOptions {
                split_interval: cli.query_frontend_split,
                shard_count: cli.query_frontend_shards,
            },
            Arc::new(query_cache),
        );
    let state = if let Some(status) = status_wal {
        state.with_wal_head_status(status.head, status.readiness)
    } else {
        state
    };
    let state = state.with_query_limits(load_runtime_overrides(cli.runtime_overrides.as_deref())?);
    let router = prometheus_router(Arc::new(state))
        .merge(mimir_tenant_admin_router(MimirTenantAdminState::new(
            store,
            metric_store,
            head,
        )))
        .merge(readiness_router(readiness));
    let serving =
        serve_prometheus_router_joinable(cli.listen, router, security, shutdown.signalled())
            .await?;
    tracing::info!(
        %serving.addr,
        tls = security.tls_enabled(),
        authentication = security.authentication_enabled(),
        "metrics-service query-frontend listening"
    );
    let outcome = crate::server_or_critical_task_exit!(serving.server, tasks);
    tasks.shutdown().await;
    outcome
}

struct BundledRulesInstallation<'a> {
    path: &'a std::path::Path,
    gate: ReadinessGate,
}

struct RulerStartup<Consumer, Coordination> {
    state_consumer: Consumer,
    coordination: Coordination,
}

#[tracing::instrument(
    level = "info",
    name = "metrics.run_ruler",
    skip_all,
    fields(listen = %launch.cli.listen, tenant = %launch.cli.ruler_tenant, shard_index = launch.cli.ruler_shard_index, shard_total = launch.cli.ruler_shard_total),
    err
)]
pub(crate) async fn run_ruler(
    launch: RoleLaunch,
    security: &ServerSecurity,
) -> Result<(), Box<dyn std::error::Error>> {
    let RoleLaunch {
        cli,
        metrics,
        readiness,
        wal_security,
        audit,
    } = launch;
    let ruler_metrics = metrics.clone();
    let role_store = RoleObjectStore::open(&cli, &metrics, &readiness)
        .map_err(|error| -> Box<dyn std::error::Error> { error })?;
    let config_store = Arc::clone(&role_store.store);
    let metric_store = role_store.refreshing_metric_store(&cli, WalHead::new());
    let state = serving_api_state(Arc::new(metric_store), &cli)
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
        .map(|path| BundledRulesInstallation {
            path,
            gate: readiness.gate("bundled-rules"),
        });
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
    let serving =
        serve_prometheus_router_joinable(cli.listen, router, security, shutdown.signalled())
            .await?;
    let bound = serving.addr;
    let server = serving.server;
    tracing::info!(
        %bound,
        tls = security.tls_enabled(),
        authentication = security.authentication_enabled(),
        "metrics-service ruler listening"
    );

    let startup = async {
        // Before the ruler reaches Kafka, so a rule file an operator names but
        // the ruler cannot install stops the start early.
        if let Some(bundled_rules) = bundled_rules {
            let path = bundled_rules.path;
            let groups =
                install_bundled_rule_groups(bound, security, path, &cli.ruler_tenant).await?;
            bundled_rules.gate.mark_ready();
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
                .auto_offset_reset(AutoOffsetReset::Earliest).isolation_level(IsolationLevel::ReadCommitted)
                .subscribe([cli.ruler_state_topic.clone()])
                .enable_auto_commit(false)
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
        Ok::<_, Box<dyn std::error::Error>>(Some(RulerStartup {
            state_consumer,
            coordination,
        }))
    };
    let clients = match startup.await {
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
    let mut state_consumer = clients.state_consumer;
    let coordination = clients.coordination;
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
    let coordination_identity = ruler_coordination_identity(&cli)?;
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
            coordination_identity.role,
            coordination_identity.member,
            shard,
            RulerTopics {
                wal: cli.wal_topic.clone(),
                state: cli.ruler_state_topic.clone(),
            },
            interval,
            coordination_identity.lease_config,
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
    let outcome = crate::server_or_critical_task_exit!(server, tasks);
    shutdown.trigger();
    tasks.shutdown().await;
    let drain_result = alert_sink.shutdown().await;
    outcome?;
    drain_result?;
    Ok(())
}

struct RulerCoordinationIdentity {
    role: Role,
    member: MemberId,
    lease_config: LeaseConfig,
}

fn ruler_coordination_identity(
    cli: &Cli,
) -> Result<RulerCoordinationIdentity, Box<dyn std::error::Error>> {
    let role_name = format!(
        "krabka-metrics-ruler-{}-{}-of-{}",
        cli.ruler_state_topic, cli.ruler_shard_index, cli.ruler_shard_total
    );
    let replica = cli
        .ruler_replica_id
        .clone()
        .unwrap_or_else(|| cli.wal_client_id.clone());
    Ok(RulerCoordinationIdentity {
        role: Role::new(&role_name)?,
        member: MemberId::new(&replica)?,
        lease_config: LeaseConfig::new(
            cli.ruler_lease_duration,
            cli.ruler_lease_renew_interval,
            cli.ruler_lease_challenge_stagger,
        )?,
    })
}

fn ruler_alert_sink(cli: &Cli) -> RulerAlertmanagerSink {
    let mut external_labels = std::collections::BTreeMap::new();
    for label in cli.ruler_external_label.iter().chain(
        cli.ruler_external_label_env
            .iter()
            .flat_map(|labels| &labels.0),
    ) {
        external_labels.insert(label.name.clone(), label.value.clone());
    }
    RulerAlertmanagerSink::from_endpoints(
        cli.ruler_alertmanager_url.clone(),
        external_labels,
        cli.ruler_generator_url_template.clone(),
        cli.ruler_alertmanager_queue_capacity,
    )
}
