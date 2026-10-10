use krabka_blockstore::ObjectStoreAccess;

use super::{
    Arc, BufferedLogHotTail, CancellationToken, JoinHandle, ObjectStore, OverridesProvider, Router,
    ServiceConfig, ServiceConfigError, ServiceDependencies, ServiceMetrics,
    SharedLogDeleteRequests, SharedLokiRules, build_configured_object_store,
    build_configured_querier_state, build_querier_state_with_overrides,
    load_querier_shared_compaction_frontier, loki_query_routes, querier_object_store_prefix,
    spawn_compaction_frontier_refresher, spawn_log_hot_tail_poller,
    spawn_wal_hot_tail_connect_and_poll,
};
use crate::{
    ConfiguredObjectStore, QuerierState, RoleReadiness, SharedCompactionFrontier, spawn_logs_ruler,
};

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
    // The querier only reads blocks and the compaction frontier. Its rules and
    // delete requests live under `data_root`, so its credential needs no write
    // access to the object store.
    let configured_store = if object_store.is_none() {
        build_configured_object_store(
            config,
            metrics.object_store.clone(),
            ObjectStoreAccess::ReadOnly,
        )
        .await?
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
        let started = FrontierRefresh {
            config,
            configured_store: configured_store.as_ref(),
            object_store,
            token: &token,
        }
        .start_hot_tail(&mut background_tasks)
        .await?;
        background_tasks.push((
            "querier WAL hot-tail",
            spawn_log_hot_tail_poller(
                wal_consumer,
                started.hot_tail.clone(),
                started.frontier.clone(),
                config.querier_hot_tail_interval,
                token.clone(),
            ),
        ));
        state = started.serve_from(state);
    } else if let Some(deferred) = dependencies.deferred_wal_consumer_connect {
        // A deferred querier cannot answer correctly without the hot tail it
        // reads recent logs from. The task that connects it marks this gate,
        // so `/ready` reports real progress. The authorizer has a gate of its
        // own, from `query_authorizer_for_role`.
        let wal_tail = readiness.gate("wal-tail");
        // Deferred connect: the consumer connects asynchronously so the
        // querier's HTTP port binds without waiting for the broker to be ready (FIX B2).
        let started = FrontierRefresh {
            config,
            configured_store: configured_store.as_ref(),
            object_store,
            token: &token,
        }
        .start_hot_tail(&mut background_tasks)
        .await?;

        // Spawn the consumer connect + poll loop in a background task.
        background_tasks.push((
            "querier WAL hot-tail",
            spawn_wal_hot_tail_connect_and_poll(
                deferred,
                started.hot_tail.clone(),
                started.frontier.clone(),
                token.clone(),
                config.querier_hot_tail_interval,
                config.querier_dependency_reconnect_interval,
                wal_tail,
            ),
        ));

        state = started.serve_from(state);
    }
    state = state.with_metrics(metrics);
    background_tasks.push(("logs ruler", spawn_logs_ruler(state.clone(), token)));
    Ok((loki_query_routes(state), background_tasks))
}

/// What a querier needs to load the shared compaction frontier its hot tail
/// filters by, and to keep that frontier moving.
struct FrontierRefresh<'a> {
    config: &'a ServiceConfig,
    configured_store: Option<&'a ConfiguredObjectStore>,
    object_store: Option<&'a dyn ObjectStore>,
    token: &'a CancellationToken,
}

/// The frontier a querier loaded, and the task that keeps it moving when the
/// store can refresh it.
struct LoadedFrontier {
    frontier: Option<SharedCompactionFrontier>,
    refresher: Option<JoinHandle<()>>,
}

/// The name a querier's frontier refresher runs under.
const FRONTIER_REFRESHER: &str = "querier compaction frontier";

/// A querier's WAL hot tail, and the compaction frontier it filters by.
struct StartedHotTail {
    hot_tail: BufferedLogHotTail,
    frontier: Option<SharedCompactionFrontier>,
}

impl StartedHotTail {
    /// `state`, answering recent reads from this hot tail.
    fn serve_from(self, state: QuerierState) -> QuerierState {
        match self.frontier {
            Some(frontier) => state.with_hot_tail_shared_frontier(self.hot_tail, frontier),
            None => state.with_hot_tail(self.hot_tail, i64::MIN),
        }
    }
}

impl FrontierRefresh<'_> {
    /// Creates the querier's hot tail and loads the frontier it filters by.
    /// The frontier's refresher, when the store can refresh it, joins
    /// `background_tasks`.
    async fn start_hot_tail(
        self,
        background_tasks: &mut Vec<(&'static str, JoinHandle<()>)>,
    ) -> Result<StartedHotTail, ServiceConfigError> {
        let hot_tail =
            BufferedLogHotTail::with_bucket_width(self.config.querier_hot_tail_bucket_width);
        let LoadedFrontier {
            frontier,
            refresher,
        } = self.load(&hot_tail).await?;
        // Named and kept, not dropped: the refresher is the only thing that
        // moves the querier's frontier forward, and a querier answering from
        // a frozen frontier says nothing about it.
        background_tasks.extend(refresher.map(|task| (FRONTIER_REFRESHER, task)));
        Ok(StartedHotTail { hot_tail, frontier })
    }

    /// Loads the frontier and, when the store can refresh it, spawns the task
    /// that does.
    async fn load(
        self,
        hot_tail: &BufferedLogHotTail,
    ) -> Result<LoadedFrontier, ServiceConfigError> {
        let (frontier, refresh_source) = load_querier_shared_compaction_frontier(
            self.config,
            self.configured_store,
            self.object_store,
        )
        .await?;
        let refresher = match (frontier.clone(), refresh_source) {
            (Some(frontier), Some((store, prefix))) => Some(spawn_compaction_frontier_refresher(
                store,
                prefix,
                frontier,
                hot_tail.clone(),
                self.token.clone(),
                self.config.querier_frontier_refresh_interval,
            )),
            _ => None,
        };
        Ok(LoadedFrontier {
            frontier,
            refresher,
        })
    }
}
