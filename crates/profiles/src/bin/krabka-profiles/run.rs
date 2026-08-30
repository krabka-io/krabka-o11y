use super::*;

#[allow(clippy::too_many_lines)]
pub(crate) async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let (client_dispatch_queue_capacity, client_frame_max) = client_resource_policy(&cli);
    let debuginfod_config = debuginfod_config(&cli)?;
    let metrics = ServiceMetrics::new();
    let admin = krabka_telemetry::profiling::spawn_admin_with_config(
        cli.admin_listen_addr,
        krabka_profiles::metrics::metrics_router(metrics.registry.clone()),
        cli.profiling.clone(),
    )
    .await?;

    let role = async move {
        match cli.target {
            Target::Distributor => {
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
                let bound = serve_supervised(cli.listen, state, shutdown.clone()).await?;
                tracing::info!(%bound, "profiles distributor listening");
                shutdown.cancelled().await;
            }
            Target::BlockBuilder => {
                let configured = build_object_store(&cli.object_store_url)
                    .map_err(|e| format!("object store: {e}"))?;
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
                krabka_profiles::blockbuilder::run_with_config(config).await?;
            }
            Target::Querier => {
                let shutdown = role_shutdown_token();
                let overrides = load_profiles_limits_overrides_config(
                    cli.profiles_limits_overrides_config.as_deref(),
                )?;
                let configured = build_object_store(&cli.object_store_url)
                    .map_err(|e| format!("object store: {e}"))?;
                let index_key = configured.object_key(&cli.index_object_key);
                let index = ProfileIndex::load_latest_snapshot_or_empty_with_max_bytes(
                    &configured.store,
                    &index_key,
                    cli.index_snapshot_max,
                )
                .await?;
                let refresh_store = Arc::clone(&configured.store);
                let cold = Arc::new(ColdProfileStore::new_with_debuginfod_config(
                    configured.store,
                    Arc::new(index),
                    cli.debuginfod_urls.clone(),
                    debuginfod_config,
                )?);
                spawn_profile_index_refresh(
                    Arc::clone(&cold),
                    refresh_store,
                    index_key.clone(),
                    cli.index_snapshot_max,
                    cli.index_refresh_interval,
                    shutdown.clone(),
                );
                let hot = WalTailProfileStore::with_retention(RetentionConfig {
                    max_age: cli.hot_store_max_age,
                    max_records: cli.hot_store_max_records,
                });
                let wal_tail = spawn_wal_tail(
                    &cli,
                    hot.clone(),
                    client_dispatch_queue_capacity,
                    client_frame_max,
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
                let bound = serve_querier(cli.listen, state, shutdown.clone()).await?;
                tracing::info!(%bound, "profiles querier listening");
                tokio::select! {
                    () = shutdown.cancelled() => {}
                    result = wal_tail => {
                        shutdown.cancel();
                        result??;
                    }
                }
            }
            Target::QueryFrontend => {
                let shutdown = role_shutdown_token();
                let overrides = load_profiles_limits_overrides_config(
                    cli.profiles_limits_overrides_config.as_deref(),
                )?;
                let configured = build_object_store(&cli.object_store_url)
                    .map_err(|e| format!("object store: {e}"))?;
                let index_key = configured.object_key(&cli.index_object_key);
                let index = ProfileIndex::load_latest_snapshot_or_empty_with_max_bytes(
                    &configured.store,
                    &index_key,
                    cli.index_snapshot_max,
                )
                .await?;
                let refresh_store = Arc::clone(&configured.store);
                let cold = Arc::new(ColdProfileStore::new_with_debuginfod_config(
                    configured.store,
                    Arc::new(index),
                    cli.debuginfod_urls.clone(),
                    debuginfod_config,
                )?);
                spawn_profile_index_refresh(
                    Arc::clone(&cold),
                    refresh_store,
                    index_key.clone(),
                    cli.index_snapshot_max,
                    cli.index_refresh_interval,
                    shutdown.clone(),
                );
                let hot = WalTailProfileStore::with_retention(RetentionConfig {
                    max_age: cli.hot_store_max_age,
                    max_records: cli.hot_store_max_records,
                });
                let wal_tail = spawn_wal_tail(
                    &cli,
                    hot.clone(),
                    client_dispatch_queue_capacity,
                    client_frame_max,
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
                let bound = serve_querier(cli.listen, state, shutdown.clone()).await?;
                tracing::info!(
                    %bound,
                    shard_width = %cli.query_frontend_shard_width.human(),
                    "profiles query-frontend listening"
                );
                tokio::select! {
                    () = shutdown.cancelled() => {}
                    result = wal_tail => {
                        shutdown.cancel();
                        result??;
                    }
                }
            }
            Target::Symbolizer => {
                krabka_profiles::symbolizer::run_with_config(
                    cli.debuginfod_urls,
                    debuginfod_config,
                )
                .await?;
            }
            Target::Compactor => {
                let configured = build_object_store(&cli.object_store_url)
                    .map_err(|e| format!("object store: {e}"))?;
                let index_key = configured.object_key(&cli.index_object_key);
                let mut index = ProfileIndex::load_latest_snapshot_or_empty_with_max_bytes(
                    &configured.store,
                    &index_key,
                    cli.index_snapshot_max,
                )
                .await?;
                let downsample =
                    cli.compactor_downsample_resolution
                        .map(|resolution| DownsamplePolicy {
                            resolution_ns: resolution.nanos_i64(),
                        });
                let metas = compact_once_with_policy(
                    &configured.store,
                    &mut index,
                    cli.compactor_max_blocks_per_job,
                    downsample,
                )
                .await?;
                index
                    .save_latest_snapshot_with_retain(
                        &configured.store,
                        &index_key,
                        cli.index_snapshot_retain,
                    )
                    .await?;
                tracing::info!(
                    compacted_blocks = metas.len(),
                    downsample_resolution = ?cli.compactor_downsample_resolution,
                    "profiles compactor finished one pass"
                );
            }
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    };

    tokio::select! {
        result = role => result,
        result = krabka_telemetry::profiling::await_admin_exit(admin) => Ok(result?),
    }
}
