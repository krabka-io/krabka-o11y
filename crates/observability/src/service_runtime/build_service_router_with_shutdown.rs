use super::{
    AllowAllIngestLimiter, Arc, BufferedLogHotTail, CancellationToken, JoinHandle, ObjectStore,
    Role, Router, ServiceConfig, ServiceConfigError, ServiceDependencies, ServiceReadiness,
    SharedLogDeleteRequests, SharedLokiRules, SwappableQueryAuthorizer,
    build_configured_object_store, build_configured_querier_state, build_querier_state,
    compactor_delete_requests_for_config, compactor_router_with_delete_requests,
    distributor_router_with_sink, load_querier_shared_compaction_frontier,
    loki_router_with_readiness, querier_object_store_prefix, spawn_compaction_frontier_refresher,
    spawn_log_hot_tail_poller, spawn_query_authorizer_connect, spawn_wal_hot_tail_connect_and_poll,
};

pub(crate) async fn build_service_router_with_shutdown(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
    token: CancellationToken,
) -> Result<(Router, Vec<(&'static str, JoinHandle<()>)>), ServiceConfigError> {
    let metrics = dependencies.metrics.clone().unwrap_or_default();
    match config.target {
        Role::Distributor => {
            let sink = dependencies
                .wal_sink
                .ok_or(ServiceConfigError::MissingWalSink)?;
            let ingest_limiter = dependencies
                .ingest_limiter
                .unwrap_or_else(|| Arc::new(AllowAllIngestLimiter));
            Ok((
                distributor_router_with_sink(
                    sink,
                    ingest_limiter,
                    config.max_ingest_body,
                    config.wal_append_timeout,
                    Some(config.reject_old_samples_max_age),
                    Some(config.creation_grace_period),
                    metrics,
                ),
                Vec::new(),
            ))
        }
        Role::Querier => {
            let mut background_tasks = Vec::new();
            let mut readiness = ServiceReadiness::ready();
            let configured_store = if object_store.is_none() {
                build_configured_object_store(config)?
            } else {
                None
            };
            let mut state = if let Some(configured_store) = configured_store.as_ref() {
                build_configured_querier_state(config, configured_store).await?
            } else {
                build_querier_state(config, object_store).await?
            };
            if let Some(configured_store) = configured_store.as_ref()
                && let Some(prefix) =
                    querier_object_store_prefix(config, Some(&configured_store.prefix))?
            {
                state = state.with_cold_object_store_source(configured_store.store.clone(), prefix);
            }
            if let Some(query_authorizer) = dependencies.query_authorizer {
                state = state.with_query_authorizer_source(query_authorizer);
            }
            let delete_requests = if let Some(delete_requests) = dependencies.delete_requests {
                delete_requests
            } else {
                SharedLogDeleteRequests::from_data_root(&config.data_root)?
            };
            state = state.with_delete_requests(delete_requests);
            state = state.with_rules(SharedLokiRules::from_data_root(&config.data_root)?);
            if let Some(hot_tail) = dependencies.hot_tail {
                state = state.with_hot_tail_source(hot_tail.source, hot_tail.frontier);
            } else if let Some(wal_consumer) = dependencies.wal_consumer {
                // Pre-connected consumer supplied directly (e.g. by tests).
                let hot_tail =
                    BufferedLogHotTail::with_bucket_width(config.querier_hot_tail_bucket_width);
                let (frontier, refresh_source) = load_querier_shared_compaction_frontier(
                    config,
                    configured_store.as_ref(),
                    object_store,
                )
                .await?;
                if let (Some(frontier), Some((store, prefix))) = (frontier.clone(), refresh_source)
                {
                    spawn_compaction_frontier_refresher(
                        store,
                        prefix,
                        frontier,
                        hot_tail.clone(),
                        token.clone(),
                        config.querier_frontier_refresh_interval,
                    );
                }
                background_tasks.push((
                    "querier WAL hot-tail",
                    spawn_log_hot_tail_poller(
                        wal_consumer,
                        hot_tail.clone(),
                        frontier.clone(),
                        config.querier_hot_tail_interval,
                        token.clone(),
                    ),
                ));
                if let Some(frontier) = frontier {
                    state = state.with_hot_tail_shared_frontier(hot_tail, frontier);
                } else {
                    state = state.with_hot_tail(hot_tail, i64::MIN);
                }
            } else if let Some(deferred) = dependencies.deferred_wal_consumer_connect {
                readiness = ServiceReadiness::deferred_querier();
                // Deferred connect: the consumer and authorizer connect asynchronously so the
                // querier's HTTP port binds without waiting for the broker to be ready (FIX B2).
                let hot_tail =
                    BufferedLogHotTail::with_bucket_width(config.querier_hot_tail_bucket_width);
                let (frontier, refresh_source) = load_querier_shared_compaction_frontier(
                    config,
                    configured_store.as_ref(),
                    object_store,
                )
                .await?;
                if let (Some(frontier), Some((store, prefix))) = (frontier.clone(), refresh_source)
                {
                    spawn_compaction_frontier_refresher(
                        store,
                        prefix,
                        frontier,
                        hot_tail.clone(),
                        token.clone(),
                        config.querier_frontier_refresh_interval,
                    );
                }

                // Spawn the consumer connect + poll loop in a background task.
                background_tasks.push((
                    "querier WAL hot-tail",
                    spawn_wal_hot_tail_connect_and_poll(
                        deferred.clone(),
                        hot_tail.clone(),
                        frontier.clone(),
                        token.clone(),
                        config.querier_hot_tail_interval,
                        config.querier_dependency_reconnect_interval,
                        readiness.clone(),
                    ),
                ));

                // Fail closed until the broker-backed authorizer connects.
                let (swappable, slot) = SwappableQueryAuthorizer::new();
                background_tasks.push((
                    "querier authorization",
                    spawn_query_authorizer_connect(
                        deferred.bootstrap,
                        deferred.topic,
                        slot,
                        deferred.client_resource_policy,
                        config.querier_dependency_reconnect_interval,
                        token.clone(),
                        readiness.clone(),
                    ),
                ));
                state = state.with_query_authorizer(swappable);

                if let Some(frontier) = frontier {
                    state = state.with_hot_tail_shared_frontier(hot_tail, frontier);
                } else {
                    state = state.with_hot_tail(hot_tail, i64::MIN);
                }
            }
            state = state.with_metrics(metrics);
            Ok((
                loki_router_with_readiness(state, readiness),
                background_tasks,
            ))
        }
        Role::Compactor => {
            let delete_requests =
                compactor_delete_requests_for_config(config, dependencies.delete_requests)?;
            Ok((
                compactor_router_with_delete_requests(delete_requests),
                Vec::new(),
            ))
        }
    }
}
