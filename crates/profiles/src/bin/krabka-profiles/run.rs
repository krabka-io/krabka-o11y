use krabka_observability::{CriticalTaskError, SupervisedTasks};

use super::*;

#[allow(clippy::too_many_lines)]
pub(crate) async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let (client_dispatch_queue_capacity, client_frame_max) = client_resource_policy(&cli);
    let debuginfod_config = debuginfod_config(&cli)?;
    let metrics = ServiceMetrics::new();
    // The admin port binds first, before the role reaches its object store or
    // its broker, so `/ready` there reports the rest of the start rather than
    // "a listener exists". The roles with a data port echo the same gates on
    // it.
    let readiness = krabka_observability::RoleReadiness::new();
    let admin = krabka_telemetry::profiling::spawn_admin_with_config(
        cli.admin_listen_addr,
        krabka_profiles::metrics::metrics_router(metrics.registry.clone())
            .merge(krabka_observability::readiness_router(readiness.clone())),
        cli.profiling.clone(),
    )
    .await?;

    let role = async move {
        match cli.target {
            Target::Distributor => {
                // Nowhere to put a push until the WAL producer has a broker.
                let wal_broker = readiness.gate("wal-broker");
                let limits = load_tenant_limits_config(cli.tenant_limits_config.as_deref())?;
                let profile_overrides = load_profiles_limits_overrides_config(
                    cli.profiles_limits_overrides_config.as_deref(),
                )?;
                let producer = Producer::builder()
                    .bootstrap(&cli.bootstrap)
                    .dispatch_queue_capacity(client_dispatch_queue_capacity.get())
                    .frame_max(client_frame_max.size())
                    .build()
                    .await?;
                wal_broker.mark_ready();
                let state = Arc::new(DistributorState {
                    sink: Arc::new(KafkaSink::with_topic(Arc::new(producer), cli.wal_topic)),
                    limits,
                    profile_overrides,
                    active_series: Mutex::default(),
                    ingestion_buckets: Mutex::default(),
                    relabel: Vec::<RelabelConfig>::new(),
                    max_decompressed: cli.distributor_request_max,
                    max_tracked_tenants: cli.distributor_max_tracked_tenants,
                    legacy_decode_limits: krabka_profiles::ingest::LegacyDecodeLimits {
                        max_nodes: cli.legacy_max_nodes,
                        max_path_bytes: cli.legacy_max_path_bytes,
                        max_trie_depth: cli.legacy_max_trie_depth,
                    },
                    metrics: metrics.clone(),
                });
                let shutdown = role_shutdown_token();
                let mut tasks = SupervisedTasks::new(shutdown.clone());
                let (bound, server) =
                    serve_supervised(cli.listen, state, readiness, shutdown.clone()).await?;
                tasks.adopt("profiles distributor HTTP", server);
                tracing::info!(%bound, "profiles distributor listening");
                let outcome = tokio::select! {
                    () = shutdown.cancelled() => Ok(()),
                    name = tasks.first_unexpected_exit() => {
                        Err(Box::<dyn std::error::Error>::from(CriticalTaskError(name)))
                    }
                };
                tasks.shutdown().await;
                outcome?;
            }
            Target::BlockBuilder => {
                let object_store_gate = readiness.gate("object-store");
                let shutdown = role_shutdown_token();
                let configured =
                    build_object_store(&cli.object_store_url, metrics.object_store.clone())
                        .map_err(|e| format!("object store: {e}"))?;
                object_store_gate.mark_ready();
                let index_key = configured.object_key(&cli.index_object_key);
                let mut config =
                    BlockBuilderConfig::new(cli.bootstrap, configured.store).with_metrics(metrics);
                config.client_dispatch_queue_capacity = client_dispatch_queue_capacity;
                config.client_frame_max = client_frame_max;
                config.wal_topic = cli.wal_topic;
                config.group_id = cli.block_builder_group_id;
                config.index_key = index_key;
                config.wal_fetch_max = cli.wal_fetch_max;
                config.wal_fetch_partition_max = cli.wal_fetch_partition_max;
                config.flush_records = cli.block_builder_flush_records;
                config.flush_max_age = cli.block_builder_flush_max_age;
                config.poll_timeout = cli.wal_poll_timeout;
                config.index_snapshot_max = cli.index_snapshot_max;
                config.index_snapshot_retain = cli.index_snapshot_retain;
                krabka_profiles::blockbuilder::run_with_config(config, shutdown).await?;
            }
            Target::Querier => {
                // A querier that answers before its block index is loaded
                // returns an empty result rather than an error, and the
                // frontend in front of it cannot tell the two apart.
                let object_store_gate = readiness.gate("object-store");
                let profile_index_gate = readiness.gate("profile-index");
                let shutdown = role_shutdown_token();
                let overrides = load_profiles_limits_overrides_config(
                    cli.profiles_limits_overrides_config.as_deref(),
                )?;
                let configured =
                    build_object_store(&cli.object_store_url, metrics.object_store.clone())
                        .map_err(|e| format!("object store: {e}"))?;
                object_store_gate.mark_ready();
                let index_key = configured.object_key(&cli.index_object_key);
                let index = ProfileIndex::load_latest_snapshot_or_empty_with_max_bytes(
                    &configured.store,
                    &index_key,
                    cli.index_snapshot_max,
                )
                .await?;
                profile_index_gate.mark_ready();
                let refresh_store = Arc::clone(&configured.store);
                let cold = Arc::new(ColdProfileStore::new_with_debuginfod_config(
                    configured.store,
                    Arc::new(index),
                    cli.debuginfod_urls.clone(),
                    debuginfod_config,
                )?);
                let mut tasks = SupervisedTasks::new(shutdown.clone());
                tasks.adopt(
                    "profiles index refresher",
                    spawn_profile_index_refresh(
                        Arc::clone(&cold),
                        refresh_store,
                        index_key.clone(),
                        cli.index_snapshot_max,
                        cli.index_refresh_interval,
                        shutdown.clone(),
                    ),
                );
                let hot = WalTailProfileStore::with_retention(RetentionConfig {
                    max_age: cli.hot_store_max_age,
                    max_records: cli.hot_store_max_records,
                });
                tasks.adopt(
                    "profiles WAL tail",
                    spawn_wal_tail(
                        &cli,
                        hot.clone(),
                        client_dispatch_queue_capacity,
                        client_frame_max,
                        metrics.wal_consumer.clone(),
                    ),
                );
                let union = Arc::new(UnionProfileStore::new(Arc::new(hot), cold));
                let state = Arc::new(
                    QuerierState::new_with_overrides(union, overrides)
                        .with_heatmap_policy(
                            cli.heatmap_value_buckets,
                            cli.heatmap_time_buckets_max,
                        )
                        .with_metrics(metrics.clone()),
                );
                let (bound, server) =
                    serve_querier(cli.listen, state, readiness, shutdown.clone()).await?;
                tasks.adopt("profiles querier HTTP", server);
                tracing::info!(%bound, "profiles querier listening");
                let outcome = tokio::select! {
                    () = shutdown.cancelled() => Ok(()),
                    name = tasks.first_unexpected_exit() => {
                        Err(Box::<dyn std::error::Error>::from(CriticalTaskError(name)))
                    }
                };
                tasks.shutdown().await;
                outcome?;
            }
            Target::QueryFrontend => {
                // A querier that answers before its block index is loaded
                // returns an empty result rather than an error, and the
                // frontend in front of it cannot tell the two apart.
                let object_store_gate = readiness.gate("object-store");
                let profile_index_gate = readiness.gate("profile-index");
                let shutdown = role_shutdown_token();
                let overrides = load_profiles_limits_overrides_config(
                    cli.profiles_limits_overrides_config.as_deref(),
                )?;
                let configured =
                    build_object_store(&cli.object_store_url, metrics.object_store.clone())
                        .map_err(|e| format!("object store: {e}"))?;
                object_store_gate.mark_ready();
                let index_key = configured.object_key(&cli.index_object_key);
                let index = ProfileIndex::load_latest_snapshot_or_empty_with_max_bytes(
                    &configured.store,
                    &index_key,
                    cli.index_snapshot_max,
                )
                .await?;
                profile_index_gate.mark_ready();
                let refresh_store = Arc::clone(&configured.store);
                let cold = Arc::new(ColdProfileStore::new_with_debuginfod_config(
                    configured.store,
                    Arc::new(index),
                    cli.debuginfod_urls.clone(),
                    debuginfod_config,
                )?);
                let mut tasks = SupervisedTasks::new(shutdown.clone());
                tasks.adopt(
                    "profiles index refresher",
                    spawn_profile_index_refresh(
                        Arc::clone(&cold),
                        refresh_store,
                        index_key.clone(),
                        cli.index_snapshot_max,
                        cli.index_refresh_interval,
                        shutdown.clone(),
                    ),
                );
                let hot = WalTailProfileStore::with_retention(RetentionConfig {
                    max_age: cli.hot_store_max_age,
                    max_records: cli.hot_store_max_records,
                });
                tasks.adopt(
                    "profiles WAL tail",
                    spawn_wal_tail(
                        &cli,
                        hot.clone(),
                        client_dispatch_queue_capacity,
                        client_frame_max,
                        metrics.wal_consumer.clone(),
                    ),
                );
                let union = Arc::new(UnionProfileStore::new(Arc::new(hot), cold));
                let state = Arc::new(
                    QuerierState::new_frontend_with_overrides(
                        union,
                        FrontendConfig {
                            shard_width: cli.query_frontend_shard_width,
                        },
                        overrides,
                    )
                    .with_heatmap_policy(cli.heatmap_value_buckets, cli.heatmap_time_buckets_max)
                    .with_metrics(metrics.clone()),
                );
                let (bound, server) =
                    serve_querier(cli.listen, state, readiness, shutdown.clone()).await?;
                tasks.adopt("profiles query-frontend HTTP", server);
                tracing::info!(
                    %bound,
                    shard_width = %cli.query_frontend_shard_width.human(),
                    "profiles query-frontend listening"
                );
                let outcome = tokio::select! {
                    () = shutdown.cancelled() => Ok(()),
                    name = tasks.first_unexpected_exit() => {
                        Err(Box::<dyn std::error::Error>::from(CriticalTaskError(name)))
                    }
                };
                tasks.shutdown().await;
                outcome?;
            }
            Target::Symbolizer => {
                krabka_profiles::symbolizer::run_with_config(
                    cli.debuginfod_urls,
                    debuginfod_config,
                )
                .await?;
            }
            Target::Compactor => {
                let object_store_gate = readiness.gate("object-store");
                let configured =
                    build_object_store(&cli.object_store_url, metrics.object_store.clone())
                        .map_err(|e| format!("object store: {e}"))?;
                object_store_gate.mark_ready();
                let index_key = configured.object_key(&cli.index_object_key);
                let policy = compaction_policy_from_cli(&cli);
                let downsample =
                    cli.compactor_downsample_resolution
                        .map(|resolution| DownsamplePolicy {
                            resolution_ns: resolution.nanos_i64(),
                        });
                let shutdown = role_shutdown_token();
                tracing::info!(
                    interval = %cli.compactor_interval.human(),
                    max_blocks_per_job = policy.max_blocks_per_job(),
                    target_rows = policy.target_rows_per_block(),
                    max_level = %policy.max_level(),
                    "profiles compactor scheduling passes"
                );
                // A pass reloads the index rather than carrying one across
                // ticks: the block builder publishes new blocks into the same
                // snapshot chain, and a stale in-memory copy would plan
                // against blocks that have since been replaced.
                let mut tick = tokio::time::interval(cli.compactor_interval.to_std());
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        biased;
                        () = shutdown.cancelled() => break,
                        _ = tick.tick() => {}
                    }
                    let started = std::time::Instant::now();
                    let outcome = run_compaction_pass(
                        &configured.store,
                        &index_key,
                        &cli,
                        policy,
                        downsample,
                        &metrics,
                    )
                    .await;
                    metrics
                        .compaction
                        .record_run(outcome.is_ok(), Time::from_std(started.elapsed()));
                    match outcome {
                        Ok(compacted_blocks) => tracing::info!(
                            compacted_blocks,
                            downsample_resolution = ?cli.compactor_downsample_resolution,
                            "profiles compactor finished one pass"
                        ),
                        // One failed pass is not a reason to lose the role. The
                        // next tick reloads the index and replans from
                        // whatever is durable. The counter is what makes that
                        // visible: without it a role whose every pass fails
                        // exports what a role with nothing to do exports.
                        Err(error) => {
                            tracing::warn!(%error, "profiles compaction pass failed; retrying on the next tick");
                        }
                    }
                }
            }
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    };

    tokio::select! {
        result = role => result,
        result = krabka_telemetry::profiling::await_admin_exit(admin) => Ok(result?),
    }
}
