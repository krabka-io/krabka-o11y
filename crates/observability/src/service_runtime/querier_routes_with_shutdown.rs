use super::{
    Arc, BufferedLogHotTail, CancellationToken, JoinHandle, ObjectStore, OverridesProvider, Router,
    ServiceConfig, ServiceConfigError, ServiceDependencies, ServiceMetrics,
    SharedLogDeleteRequests, SharedLokiRules, build_configured_object_store,
    build_configured_querier_state, build_querier_state_with_overrides,
    load_querier_shared_compaction_frontier, loki_query_routes, querier_object_store_prefix,
    spawn_compaction_frontier_refresher, spawn_log_hot_tail_poller,
    spawn_wal_hot_tail_connect_and_poll,
};
use crate::{RoleReadiness, spawn_logs_ruler};

/// The querier's read routes, and the tasks that keep them able to answer.
///
/// The routes come back without the ops routes on them, so that a
/// single-role querier can wrap them in its own identity and an all-in-one can
/// merge them with the distributor's write routes under one. The tasks are
/// returned rather than detached: each of them is something the querier cannot
/// serve correct answers without, so the caller supervises them.
///
/// # Errors
/// Returns an error when the configured object store or index cannot be built.
pub(crate) async fn querier_routes_with_shutdown(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
    token: CancellationToken,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    overrides: Arc<OverridesProvider>,
) -> Result<(Router, Vec<(&'static str, JoinHandle<()>)>), ServiceConfigError> {
    let mut background_tasks = Vec::new();
    let configured_store = if object_store.is_none() {
        build_configured_object_store(config, metrics.object_store.clone())?
    } else {
        None
    };
    let mut state = if let Some(configured_store) = configured_store.as_ref() {
        build_configured_querier_state(config, configured_store, overrides).await?
    } else {
        build_querier_state_with_overrides(config, object_store, overrides).await?
    };
    if let Some(configured_store) = configured_store.as_ref()
        && let Some(prefix) = querier_object_store_prefix(config, Some(&configured_store.prefix))?
    {
        state = state.with_cold_object_store_source(configured_store.store.clone(), prefix);
    }
    // The ruler routes share this state, so they check tenants against the
    // same authorizer as the reads. The caller resolves which authorizer that
    // is, with `query_authorizer_for_role`.
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
        let hot_tail = BufferedLogHotTail::with_bucket_width(config.querier_hot_tail_bucket_width);
        let (frontier, refresh_source) = load_querier_shared_compaction_frontier(
            config,
            configured_store.as_ref(),
            object_store,
        )
        .await?;
        if let (Some(frontier), Some((store, prefix))) = (frontier.clone(), refresh_source) {
            // Named and kept, not dropped: this task is the only thing
            // that moves the querier's frontier forward, and a querier
            // answering from a frozen frontier says nothing about it.
            background_tasks.push((
                "querier compaction frontier",
                spawn_compaction_frontier_refresher(
                    store,
                    prefix,
                    frontier,
                    hot_tail.clone(),
                    token.clone(),
                    config.querier_frontier_refresh_interval,
                ),
            ));
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
        // A deferred querier cannot answer correctly without the hot tail it
        // reads recent logs from. The task that connects it marks this gate,
        // so `/ready` reports real progress. The authorizer has a gate of its
        // own, from `query_authorizer_for_role`.
        let wal_tail = readiness.gate("wal-tail");
        // Deferred connect: the consumer connects asynchronously so the
        // querier's HTTP port binds without waiting for the broker to be ready (FIX B2).
        let hot_tail = BufferedLogHotTail::with_bucket_width(config.querier_hot_tail_bucket_width);
        let (frontier, refresh_source) = load_querier_shared_compaction_frontier(
            config,
            configured_store.as_ref(),
            object_store,
        )
        .await?;
        if let (Some(frontier), Some((store, prefix))) = (frontier.clone(), refresh_source) {
            // Named and kept, not dropped: this task is the only thing
            // that moves the querier's frontier forward, and a querier
            // answering from a frozen frontier says nothing about it.
            background_tasks.push((
                "querier compaction frontier",
                spawn_compaction_frontier_refresher(
                    store,
                    prefix,
                    frontier,
                    hot_tail.clone(),
                    token.clone(),
                    config.querier_frontier_refresh_interval,
                ),
            ));
        }

        // Spawn the consumer connect + poll loop in a background task.
        background_tasks.push((
            "querier WAL hot-tail",
            spawn_wal_hot_tail_connect_and_poll(
                deferred,
                hot_tail.clone(),
                frontier.clone(),
                token.clone(),
                config.querier_hot_tail_interval,
                config.querier_dependency_reconnect_interval,
                wal_tail,
            ),
        ));

        if let Some(frontier) = frontier {
            state = state.with_hot_tail_shared_frontier(hot_tail, frontier);
        } else {
            state = state.with_hot_tail(hot_tail, i64::MIN);
        }
    }
    state = state.with_metrics(metrics);
    background_tasks.push(("logs ruler", spawn_logs_ruler(state.clone(), token)));
    Ok((loki_query_routes(state), background_tasks))
}
