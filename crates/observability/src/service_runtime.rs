#[allow(clippy::wildcard_imports)]
use super::*;

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn build_service_router(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<Router, ServiceConfigError> {
    build_service_router_with_shutdown(config, dependencies, object_store, CancellationToken::new())
        .await
        .map(|(router, _)| router)
}

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

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn serve_service(
    config: ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<(), ServiceRuntimeError> {
    let listener = TcpListener::bind(config.listen_addr).await?;
    serve_service_listener(listener, config, dependencies, object_store).await
}

/// Waits for an operating-system signal requesting graceful shutdown.
///
/// On Unix, either `SIGINT` (usually sent by Ctrl+C) or `SIGTERM` resolves the
/// future. On other platforms, only the platform's Ctrl+C notification is
/// available.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to install Ctrl+C handler; triggering shutdown");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                tracing::error!(%error, "failed to install SIGTERM handler; triggering shutdown");
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
/// # Panics
/// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
pub async fn serve_service_listener(
    listener: TcpListener,
    config: ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<(), ServiceRuntimeError> {
    if config.target == Role::Compactor {
        return serve_compactor_service_listener(listener, config, dependencies, object_store)
            .await;
    }

    let token = CancellationToken::new();
    let token_sig = token.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        token_sig.cancel();
    });
    let token_srv = token.clone();
    let (app, background_tasks) =
        build_service_router_with_shutdown(&config, dependencies, object_store, token.clone())
            .await?;
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async move { token_srv.cancelled().await })
        .into_future();
    tokio::pin!(server);
    let mut tasks = tokio::task::JoinSet::new();
    for (name, handle) in background_tasks {
        tasks.spawn(async move {
            let result = handle.await;
            (name, result)
        });
    }
    if tasks.is_empty() {
        server.await?;
    } else {
        tokio::select! {
            result = &mut server => result?,
            result = tasks.join_next() => {
                if token.is_cancelled() {
                    server.await?;
                } else {
                    let name = result
                        .and_then(Result::ok)
                        .map_or("unknown", |(name, _)| name);
                    token.cancel();
                    return Err(ServiceRuntimeError::CriticalTask(name));
                }
            }
        }
    }
    token.cancel();
    while tasks.join_next().await.is_some() {}
    Ok(())
}

pub(crate) async fn serve_compactor_service_listener(
    listener: TcpListener,
    config: ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<(), ServiceRuntimeError> {
    let delete_requests =
        compactor_delete_requests_for_config(&config, dependencies.delete_requests.clone())?;
    let app = compactor_router_with_delete_requests(delete_requests.clone());
    let dependencies = dependencies.with_delete_requests(delete_requests);
    let (http_shutdown_tx, http_shutdown_rx) = tokio::sync::oneshot::channel();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = http_shutdown_rx.await;
        })
        .into_future();
    let compactor = run_compactor_until_shutdown(&config, dependencies, object_store, pending());
    tokio::pin!(server);
    tokio::pin!(compactor);

    tokio::select! {
        result = &mut server => {
            result?;
            Ok(())
        }
        result = &mut compactor => {
            let _ = http_shutdown_tx.send(());
            result?;
            Ok(())
        }
    }
}
