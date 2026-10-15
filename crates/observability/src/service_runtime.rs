use krabka_blockstore::ObjectStoreAccess;
use krabka_units::prelude::TimeExt as _;
use tokio_util::sync::DropGuard;

use crate::{
    ALL_OPS, AllowAllIngestLimiter, AllowAllQueryAuthorizer, Arc, BLOCK_BUILDER_OPS,
    BufferedLogHotTail, CancellationToken, ConfiguredObjectStore, CriticalTaskError,
    DISTRIBUTOR_OPS, DRAINING_GATE, DistributorState, JoinHandle, LogIngestLimiter,
    LogQueryAuthorizer, ObjectStore, OverridesProvider, QUERIER_OPS, QuerierState, ReadinessGate,
    Role, RoleKind, RoleReadiness, Router, ServiceAudit, ServiceConfig, ServiceConfigError,
    ServiceDependencies, ServiceMetrics, ServiceRuntimeError, SharedCompactionFrontier,
    SharedLogDeleteRequests, SharedLokiRules, StagedDrain, SupervisedTasks,
    SwappableQueryAuthorizer, TcpListener,
    audit::{AuditHandle, AuditService, AuditServiceParts, krabka_product},
    build_configured_object_store, build_configured_querier_state,
    build_querier_state_with_overrides, compactor_delete_requests_for_config,
    compactor_router_with_delete_requests, delete_request_routes, distributor_push_routes,
    distributor_router_with_sink, limits_provider_for_config,
    load_querier_shared_compaction_frontier, loki_query_routes, querier_object_store_prefix,
    run_compactor_until_idle, run_compactor_until_shutdown,
    server_security::{ServerListener, ServerSecurity, serve_router},
    spawn_compaction_frontier_refresher, spawn_log_hot_tail_poller, spawn_logs_ruler,
    spawn_query_authorizer_connect, spawn_wal_hot_tail_connect_and_poll, with_role_ops_routes,
    with_service_audit,
};

pub(crate) struct BackgroundTask {
    pub(crate) name: &'static str,
    pub(crate) handle: JoinHandle<()>,
}

pub(crate) struct ServiceRouter {
    pub(crate) router: Router,
    pub(crate) background_tasks: Vec<BackgroundTask>,
}

impl BackgroundTask {
    pub(crate) fn new(name: &'static str, handle: JoinHandle<()>) -> Self {
        Self { name, handle }
    }
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn build_service_router(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<Router, ServiceConfigError> {
    build_service_router_with_shutdown(config, dependencies, object_store, CancellationToken::new())
        .await
        .map(|service_router| service_router.router)
}

/// Spawns the task that keeps the distributor's ingest limiter fresh until
/// `token` is cancelled.
///
/// The role supervises it. A broker-backed limiter refreshes its ACL snapshot
/// here, and a limiter without cached state only waits for the cancel.
pub(crate) fn ingest_limiter_refresh_task(
    limiter: &Arc<dyn LogIngestLimiter>,
    token: &CancellationToken,
) -> BackgroundTask {
    let limiter = Arc::clone(limiter);
    let token = token.clone();
    BackgroundTask::new(
        "distributor ingest limits",
        tokio::spawn(async move { limiter.keep_fresh(token).await }),
    )
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn serve_service(
    config: ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<(), ServiceRuntimeError> {
    let listener = TcpListener::bind(config.listen_addr).await?;
    // Boxed: the role start-ups this dispatches to are several KB of future
    // between them, and inlining them here would put all of that on the stack
    // of whatever awaits `serve_service`.
    Box::pin(serve_service_listener(
        listener,
        config,
        dependencies,
        object_store,
    ))
    .await
}

/// The one data port of a `--target all` process, and the querier tasks behind
/// it.
///
/// `Loki`'s single binary serves push and query on one port, and so does this.
/// The three roles' surfaces are merged rather than served on three listeners,
/// because three listeners would mean three ports to publish, three probes to
/// wire, and a `Loki` datasource that could not simply be pointed at the
/// process. The routes do not overlap: each role's push, read and
/// delete-request routes are disjoint, and the ops routes -- which every role
/// would otherwise contribute -- are added once here, under
/// [`ALL_OPS`](crate::ALL_OPS).
///
/// # Errors
/// Returns an error when a dependency any of the three roles needs is absent,
/// or when the querier's object store or index cannot be built.
pub(crate) async fn all_in_one_router(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
    token: CancellationToken,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    distributor_state: DistributorState,
) -> Result<ServiceRouter, ServiceConfigError> {
    // The querier resolves tenants through the very provider the distributor
    // holds, so the two gates cannot answer one tenant with two limit sets.
    let overrides = Arc::clone(&distributor_state.overrides);
    let delete_requests =
        compactor_delete_requests_for_config(config, dependencies.delete_requests.clone())?;
    // Without the consumer: the one in `dependencies` is the block builder's,
    // and a querier that took it as its hot tail would poll records out of the
    // block builder's group. See
    // [`without_wal_consumer`](ServiceDependencies::without_wal_consumer).
    let querier_readiness = readiness.for_role(RoleKind::Querier);
    // One authorizer for the reads, the ruler and the delete-request API, so
    // the three surfaces cannot answer one tenant in two ways.
    let role_authorizer =
        query_authorizer_for_role(config, &dependencies, &token, &querier_readiness);
    let query_authorizer = Arc::clone(&role_authorizer.authorizer);
    let limiter_task = ingest_limiter_refresh_task(&distributor_state.ingest_limiter, &token);
    let querier = querier_routes_with_shutdown(
        config,
        ServiceDependencies {
            query_authorizer: Some(role_authorizer.authorizer),
            ..dependencies
                .without_wal_consumer()
                .with_delete_requests(delete_requests.clone())
        },
        object_store,
        token,
        metrics,
        // Scoped, so a querier precondition that is unmet reads as
        // `querier/wal-tail` rather than as a bare `wal-tail` that any of the
        // three roles could have registered.
        querier_readiness,
        overrides,
    )
    .await?;
    let mut background_tasks = querier.background_tasks;
    background_tasks.extend(role_authorizer.task);
    background_tasks.push(limiter_task);
    let router = with_role_ops_routes(Router::new(), ALL_OPS, readiness)
        .merge(distributor_push_routes(distributor_state))
        .merge(querier.router)
        .merge(delete_request_routes(delete_requests, query_authorizer));
    Ok(ServiceRouter {
        router,
        background_tasks,
    })
}

pub(crate) async fn build_service_router_with_shutdown(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
    token: CancellationToken,
) -> Result<ServiceRouter, ServiceConfigError> {
    let metrics = dependencies.metrics.clone().unwrap_or_default();
    // The binary may have handed in the same readiness its admin port serves,
    // so a probe on `:9404` and a probe on the data port agree.
    let readiness = dependencies.readiness.clone().unwrap_or_default();
    // Every route of the role records through the audit handle in
    // `dependencies`, or through a disabled one.
    let audit = service_audit_for_config(config, &dependencies);
    // One provider for the process. Both the write path and the read path
    // resolve every tenant through this one, so the two gates cannot disagree.
    let overrides = limits_provider_for_config(config)?;
    let service_router = match config.target {
        Role::Distributor => {
            let state = distributor_state_for_config(
                config,
                &dependencies,
                metrics,
                readiness
                    .for_role(RoleKind::Distributor)
                    .gate(DRAINING_GATE),
                overrides,
            )?;
            let refresh = ingest_limiter_refresh_task(&state.ingest_limiter, &token);
            ServiceRouter {
                router: distributor_router_with_sink(
                    state.sink,
                    state.ingest_limiter,
                    state.overrides,
                    config.wal_append_timeout,
                    state.metrics,
                ),
                background_tasks: vec![refresh],
            }
        }
        Role::Querier => {
            let role_authorizer =
                query_authorizer_for_role(config, &dependencies, &token, &readiness);
            let dependencies = ServiceDependencies {
                query_authorizer: Some(role_authorizer.authorizer),
                ..dependencies
            };
            let mut service_router = Box::pin(querier_routes_with_shutdown(
                config,
                dependencies,
                object_store,
                token,
                metrics,
                readiness.clone(),
                overrides,
            ))
            .await?;
            service_router.background_tasks.extend(role_authorizer.task);
            service_router.router = with_role_ops_routes(Router::new(), QUERIER_OPS, readiness)
                .merge(service_router.router);
            service_router
        }
        Role::BlockBuilder => {
            let role_authorizer =
                query_authorizer_for_role(config, &dependencies, &token, &readiness);
            let delete_requests =
                compactor_delete_requests_for_config(config, dependencies.delete_requests)?;
            ServiceRouter {
                router: compactor_router_with_delete_requests(
                    delete_requests,
                    role_authorizer.authorizer,
                    readiness,
                ),
                background_tasks: role_authorizer.task.into_iter().collect(),
            }
        }
        Role::All => {
            let distributor_state = distributor_state_for_config(
                config,
                &dependencies,
                metrics.clone(),
                readiness
                    .for_role(RoleKind::Distributor)
                    .gate(DRAINING_GATE),
                Arc::clone(&overrides),
            )?;
            Box::pin(all_in_one_router(
                config,
                dependencies,
                object_store,
                token,
                metrics,
                readiness,
                distributor_state,
            ))
            .await?
        }
    };
    Ok(ServiceRouter {
        router: with_service_audit(service_router.router, audit),
        background_tasks: service_router.background_tasks,
    })
}

/// The distributor's state, holding the gate a drain clears.
///
/// The gate is taken rather than registered here so that exactly one exists
/// per distributor. An all-in-one both serves `POST
/// /ingester/prepare_shutdown` from this state and clears the same gate as the
/// first step of its stop; registering one gate in each place would leave
/// `/ready` listing `distributor/accepting-writes` twice and an operator's
/// drain clearing only one of them, so the probe would go on reporting a
/// distributor that is willing to take writes it no longer wants.
///
/// # Errors
/// Returns [`ServiceConfigError::MissingWalSink`] when no WAL sink was built.
pub(crate) fn distributor_state_for_config(
    config: &ServiceConfig,
    dependencies: &ServiceDependencies,
    metrics: ServiceMetrics,
    accepting_writes: ReadinessGate,
    overrides: Arc<OverridesProvider>,
) -> Result<DistributorState, ServiceConfigError> {
    let sink = dependencies
        .wal_sink
        .clone()
        .ok_or(ServiceConfigError::MissingWalSink)?;
    let ingest_limiter = dependencies
        .ingest_limiter
        .clone()
        .unwrap_or_else(|| Arc::new(AllowAllIngestLimiter));
    // Met from the start: a distributor that has its WAL sink can take writes.
    // An operator's drain request, or the first step of an all-in-one's stop,
    // is what drops it.
    accepting_writes.mark_ready();
    Ok(DistributorState {
        sink,
        ingest_limiter,
        prepare_shutdown: accepting_writes,
        overrides,
        wal_append_timeout: config.wal_append_timeout,
        metrics,
    })
}

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
) -> Result<ServiceRouter, ServiceConfigError> {
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
        background_tasks.push(BackgroundTask::new(
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
        background_tasks.push(BackgroundTask::new(
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
    background_tasks.push(BackgroundTask::new(
        "logs ruler",
        spawn_logs_ruler(state.clone(), token),
    ));
    Ok(ServiceRouter {
        router: loki_query_routes(state),
        background_tasks,
    })
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
        background_tasks: &mut Vec<BackgroundTask>,
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
        background_tasks
            .extend(refresher.map(|task| BackgroundTask::new(FRONTIER_REFRESHER, task)));
        Ok(StartedHotTail { hot_tail, frontier })
    }

    /// Loads the frontier and, when the store can refresh it, spawns the task
    /// that does.
    async fn load(
        self,
        hot_tail: &BufferedLogHotTail,
    ) -> Result<LoadedFrontier, ServiceConfigError> {
        let loaded = load_querier_shared_compaction_frontier(
            self.config,
            self.configured_store,
            self.object_store,
        )
        .await?;
        let refresher = match loaded.frontier.clone() {
            Some(frontier) => loaded.refresh_source.map(|source| {
                spawn_compaction_frontier_refresher(
                    source.store,
                    source.prefix,
                    frontier,
                    hot_tail.clone(),
                    self.token.clone(),
                    self.config.querier_frontier_refresh_interval,
                )
            }),
            None => None,
        };
        Ok(LoadedFrontier {
            frontier: loaded.frontier,
            refresher,
        })
    }
}

/// The query authorizer a querier or a block builder checks tenants against,
/// and the task that connects it, when there is one.
///
/// An authorizer in `dependencies` is used as it is. Otherwise, with a broker
/// to connect to, the role gets a [`SwappableQueryAuthorizer`] that fails
/// closed until the broker-backed authorizer connects. The role must
/// supervise the returned task: it is also what keeps the ACL snapshot fresh.
/// `readiness` gets a `query-authorization` gate that the connect marks.
///
/// With no broker at all, which is an embedded router or a test, every tenant
/// is allowed, as [`crate::QuerierState::new`] allows every tenant.
pub(crate) fn query_authorizer_for_role(
    config: &ServiceConfig,
    dependencies: &ServiceDependencies,
    token: &CancellationToken,
    readiness: &RoleReadiness,
) -> RoleQueryAuthorizer {
    if let Some(authorizer) = &dependencies.query_authorizer {
        return RoleQueryAuthorizer {
            authorizer: Arc::clone(authorizer),
            task: None,
        };
    }
    let Some(connect) = dependencies.deferred_query_authorizer_connect.clone() else {
        return RoleQueryAuthorizer {
            authorizer: Arc::new(AllowAllQueryAuthorizer),
            task: None,
        };
    };
    let parts = SwappableQueryAuthorizer::unavailable();
    let swappable = parts.authorizer;
    let slot = parts.slot;
    let task = spawn_query_authorizer_connect(
        connect,
        slot,
        config.querier_dependency_reconnect_interval,
        token.clone(),
        readiness.gate("query-authorization"),
    );
    RoleQueryAuthorizer {
        authorizer: Arc::new(swappable),
        task: Some(BackgroundTask::new("query authorization", task)),
    }
}

/// The query authorizer a role checks tenants against, and the task that
/// connects it and keeps it fresh, when the role has one.
pub(crate) struct RoleQueryAuthorizer {
    pub(crate) authorizer: Arc<dyn LogQueryAuthorizer>,
    pub(crate) task: Option<BackgroundTask>,
}

/// The security of one serving role: the posture of its data port, its audit
/// handle, and the writer task behind that handle.
pub(crate) struct RuntimeSecurity {
    /// The posture that the data-port listener serves with. It reports its
    /// security events to `audit`.
    pub(crate) server: ServerSecurity,
    /// The handle that the handlers record audit events through.
    pub(crate) audit: AuditHandle,
    /// The audit writer, when the role started the audit layer itself.
    pub(crate) audit_writer: Option<JoinHandle<()>>,
    /// Stops the audit writer. Cancel it after the listener stops, so that no
    /// request records an event after the audit queue stops.
    pub(crate) audit_stop: CancellationToken,
}

/// Serves every logs role from one process, and stops them in an order that
/// does not lose a push.
///
/// The three roles share a port and a stop. The stop is staged, because the
/// obvious alternative -- one token for all of them -- ends the block builder
/// at the same instant as the distributor, and the records the distributor
/// accepted in its last second are then left in the WAL for a restart that,
/// on a laptop, never comes:
///
/// 1. **The data port drains.** Its graceful shutdown stops accepting and lets
///    the requests already in flight finish. A push is acknowledged only after
///    its WAL append has been, so a request that got a 204 is in the WAL
///    before this step ends. The drain gate is cleared as the listener closes,
///    so `/ready` reports the drain for whatever is still probing.
/// 2. **The querier's background tasks stop.** They read; there is nothing to
///    lose.
/// 3. **The block builder empties the WAL.** Nothing is adding to it by now,
///    so [`run_compactor_until_idle`] terminates: everything the process
///    accepted is in a block before it exits.
///
/// `shutdown` is the process's stop request, which the binary cancels on
/// `SIGTERM`. It is a token rather than a signal handler so that a test, or a
/// process that embeds the stack, can ask for the same staged stop and watch
/// what it leaves behind -- which is the only way to hold the order this
/// function exists to keep.
///
/// # Errors
/// Returns an error when a role cannot start, when the HTTP server fails, or
/// when a supervised task stops while the process was not shutting down.
pub async fn serve_all_service_listener(
    listener: TcpListener,
    config: ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
    shutdown: CancellationToken,
) -> Result<(), ServiceRuntimeError> {
    let audited = Box::pin(start_audited_runtime_security(&config, dependencies)).await?;
    let security = audited.security;
    let _audit_stop = audited.stop_guard;
    let dependencies = audited.dependencies;
    let readiness = dependencies.readiness.clone().unwrap_or_default();
    let metrics = dependencies.metrics.clone().unwrap_or_default();
    if let Some(consumer) = &dependencies.wal_consumer {
        consumer.lock().await.set_catch_up_gate(
            readiness
                .for_role(RoleKind::BlockBuilder)
                .gate("wal-catch-up"),
        );
    }
    // One gate for the process, registered here and handed to the router, so
    // that `POST /ingester/prepare_shutdown` and the first step of this stop
    // clear the same one.
    let accepting_writes = readiness
        .for_role(RoleKind::Distributor)
        .gate(DRAINING_GATE);
    // One provider for the process: the distributor and the querier below
    // share it, so an ingest gate and a read gate answer the same tenant with
    // the same numbers.
    let overrides = limits_provider_for_config(&config)?;
    // The block builder below reads the retention window out of the same
    // provider, so one process enforces one set of numbers.
    let dependencies = dependencies.with_limits(Arc::clone(&overrides));
    let distributor_state = distributor_state_for_config(
        &config,
        &dependencies,
        metrics.clone(),
        accepting_writes.clone(),
        Arc::clone(&overrides),
    )?;
    let querier_token = CancellationToken::new();
    let service_router = Box::pin(all_in_one_router(
        &config,
        dependencies.clone(),
        object_store,
        querier_token.clone(),
        metrics,
        readiness.clone(),
        distributor_state,
    ))
    .await?;
    let app = with_service_audit(
        service_router.router,
        service_audit_for_config(&config, &dependencies),
    );
    let mut querier_tasks = SupervisedTasks::new(querier_token);
    for task in service_router.background_tasks {
        querier_tasks.adopt(task.name, task.handle);
    }
    // Supervised with the querier's tasks, so a writer that stops early stops
    // the process. It is cancelled on its own token, after the data port.
    if let Some(writer) = security.audit_writer {
        querier_tasks.adopt("audit writer", writer);
    }

    let mut drain = StagedDrain::new(config.all_drain_stage_timeout);
    // Named for the distributor because the distributor's stop is the one the
    // order depends on. The querier's routes ride on the same listener and
    // stop with it, which costs nothing: they hold no write.
    let data_port = serve_router(
        ServerListener::bind(listener, &security.server)?,
        app,
        &security.server,
    );
    drain.stage(RoleKind::Distributor.as_str(), move |token| async move {
        let server = data_port.with_graceful_shutdown(async move {
            token.cancelled().await;
            // Before the listener closes, not after: a probe that is mid-
            // drain should read 503 rather than a connection refused.
            accepting_writes.mark_unready();
        });
        if let Err(error) = server.await {
            tracing::error!(%error, "all-in-one data port stopped");
        }
    });

    let builder_token = CancellationToken::new();
    let builder = run_compactor_until_shutdown(
        &config,
        dependencies.clone(),
        object_store,
        builder_token.clone().cancelled_owned(),
    );
    tokio::pin!(builder);

    // Held rather than awaited twice: a future that has already returned must
    // not be polled again, and the block builder is the one branch of the
    // select below that can finish before the drain starts.
    let mut finished_early = None;
    let outcome = tokio::select! {
        result = &mut builder => {
            finished_early = Some(result);
            // Whatever it reported, the process is now one role short with its
            // port still open, which is a fault whichever way the role ended.
            Err(CriticalTaskError(RoleKind::BlockBuilder.as_str()).into())
        }
        name = drain.first_unexpected_exit() => Err(CriticalTaskError(name).into()),
        name = querier_tasks.first_unexpected_exit() => Err(CriticalTaskError(name).into()),
        () = shutdown.cancelled() => Ok(()),
    };

    // The data port first, so nothing new enters the WAL, and only then the
    // roles that read it.
    for stage in drain.drain().await {
        tracing::warn!(stage, "role did not finish draining in time");
    }
    // The data port has stopped, so no request records an event after this.
    security.audit_stop.cancel();
    querier_tasks.shutdown().await;
    let builder_outcome = if let Some(result) = finished_early {
        result
    } else {
        builder_token.cancel();
        builder.await
    };
    if let Err(error) = &builder_outcome {
        tracing::error!(%error, "block builder stopped with an error");
    }
    // Last, and only now: nothing is writing the WAL any more, so this pass
    // ends rather than running forever, and it is what puts the pushes this
    // process accepted last into a block rather than leaving them in a WAL
    // that, in a one-process stack, nothing restarts to read.
    let drained = tokio::time::timeout(
        config.all_drain_stage_timeout.to_std(),
        run_compactor_until_idle(&config, dependencies, object_store),
    )
    .await
    .unwrap_or_else(|_| {
        Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "block builder could not drain the WAL before exit",
        )
        .into())
    });
    match &drained {
        Ok(blocks) => tracing::info!(
            blocks = blocks.len(),
            "block builder drained the WAL before exit"
        ),
        Err(error) => tracing::error!(%error, "block builder could not drain the WAL before exit"),
    }
    outcome
        .and(builder_outcome.map(|_| ()))
        .and(drained.map(|_| ()))
}

pub(crate) async fn serve_compactor_service_listener(
    listener: TcpListener,
    config: ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<(), ServiceRuntimeError> {
    let audited = Box::pin(start_audited_runtime_security(&config, dependencies)).await?;
    let security = audited.security;
    let _audit_stop = audited.stop_guard;
    let dependencies = audited.dependencies;
    // The block builder resolves a tenant's retention window through the same
    // provider the other roles read their limits from. Without it the sweep
    // would read no window for any tenant and delete nothing, whatever the
    // operator configured.
    let dependencies = dependencies.with_limits(limits_provider_for_config(&config)?);
    let delete_requests =
        compactor_delete_requests_for_config(&config, dependencies.delete_requests.clone())?;
    let readiness = dependencies.readiness.clone().unwrap_or_default();
    if let Some(consumer) = &dependencies.wal_consumer {
        consumer
            .lock()
            .await
            .set_catch_up_gate(readiness.gate("wal-catch-up"));
    }
    // The delete-request API checks every tenant against this authorizer. Its
    // connect task also keeps the ACL snapshot fresh, so it is supervised: a
    // block builder whose authorizer stopped would refuse every delete call
    // and say nothing. The audit writer is supervised with it, because a
    // block builder whose audit writer stopped would record no delete request.
    let mut tasks = SupervisedTasks::new(CancellationToken::new());
    let role_authorizer =
        query_authorizer_for_role(&config, &dependencies, tasks.token(), &readiness);
    if let Some(task) = role_authorizer.task {
        tasks.adopt(task.name, task.handle);
    }
    if let Some(writer) = security.audit_writer {
        tasks.adopt("audit writer", writer);
    }
    let app = with_service_audit(
        compactor_router_with_delete_requests(
            delete_requests.clone(),
            role_authorizer.authorizer,
            readiness,
        ),
        service_audit_for_config(&config, &dependencies),
    );
    let dependencies = dependencies.with_delete_requests(delete_requests);
    let shutdown_channel = tokio::sync::oneshot::channel();
    let http_shutdown_tx = shutdown_channel.0;
    let http_shutdown_rx = shutdown_channel.1;
    let listener = ServerListener::bind(listener, &security.server)?;
    let server = serve_router(listener, app, &security.server)
        .with_graceful_shutdown(async {
            let _ = http_shutdown_rx.await;
        })
        .into_future();
    // SIGTERM is how Kubernetes, systemd and `docker stop` ask a process to
    // stop. Waiting on a future that never resolves left the compactor to be
    // SIGKILLed at the end of the grace period, mid-batch: the block it was
    // writing was abandoned and its WAL offset went uncommitted, so every
    // ordinary restart replayed that window.
    let compactor =
        run_compactor_until_shutdown(&config, dependencies, object_store, shutdown_signal());
    tokio::pin!(server);
    tokio::pin!(compactor);

    let outcome = tokio::select! {
        result = &mut server => result.map_err(ServiceRuntimeError::from),
        result = &mut compactor => {
            // The compactor has finished its batch and committed. Stop
            // accepting, let the requests already in flight finish, and only
            // then report how the compactor ended.
            let _ = http_shutdown_tx.send(());
            if let Err(error) = server.await {
                tracing::warn!(%error, "compactor HTTP server stopped with an error while draining");
            }
            result.map(|_| ())
        }
        name = tasks.first_unexpected_exit() => {
            let _ = http_shutdown_tx.send(());
            if let Err(error) = server.await {
                tracing::warn!(%error, "compactor HTTP server stopped with an error while draining");
            }
            Err(CriticalTaskError(name).into())
        }
    };
    // The listener has stopped, so no request records an event after this.
    security.audit_stop.cancel();
    tasks.shutdown().await;
    outcome
}

/// Serves a role on `listener` until it is asked to stop or one of its
/// background tasks stops first.
///
/// The listener and the role's background tasks are joined, not detached. A
/// task that returns, errors, or panics cancels the shutdown token and fails
/// this call with [`ServiceRuntimeError::CriticalTask`], so the process exits
/// instead of holding the port open over a WAL consumer that is no longer
/// consuming.
///
/// A panic inside a request handler is the opposite case and is contained: the
/// router answers 500 for that request and keeps the connection.
///
/// The listener serves TLS and authenticates requests as the
/// `server_security` flags of `config` say, and it serves plain HTTP with no
/// authentication when they are unset. The role records its audit events as
/// the `audit` flags say, and it supervises the audit writer with its other
/// tasks.
///
/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, the configured storage or export backend fails, a security flag does not load, the audit layer does not start, or a critical background task stops.
pub async fn serve_service_listener(
    listener: TcpListener,
    config: ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<(), ServiceRuntimeError> {
    if config.target == Role::BlockBuilder {
        return Box::pin(serve_compactor_service_listener(
            listener,
            config,
            dependencies,
            object_store,
        ))
        .await;
    }
    if config.target == Role::All {
        let token = CancellationToken::new();
        let token_sig = token.clone();
        // Not supervised: this task is meant to finish, and finishing is how
        // it does its job.
        tokio::spawn(async move {
            shutdown_signal().await;
            token_sig.cancel();
        });
        // Boxed: this future carries the whole all-in-one start-up, which is
        // several KB, and would otherwise be inlined into every caller of
        // `serve_service_listener` including the single-role ones.
        return Box::pin(serve_all_service_listener(
            listener,
            config,
            dependencies,
            object_store,
            token,
        ))
        .await;
    }

    let audited = Box::pin(start_audited_runtime_security(&config, dependencies)).await?;
    let security = audited.security;
    let _audit_stop = audited.stop_guard;
    let dependencies = audited.dependencies;
    let token = CancellationToken::new();
    let token_sig = token.clone();
    // Not supervised: this task is meant to finish, and finishing is how it
    // does its job.
    tokio::spawn(async move {
        shutdown_signal().await;
        token_sig.cancel();
    });
    let token_srv = token.clone();
    let service_router =
        build_service_router_with_shutdown(&config, dependencies, object_store, token.clone())
            .await?;
    let listener = ServerListener::bind(listener, &security.server)?;
    let server = serve_router(listener, service_router.router, &security.server)
        .with_graceful_shutdown(async move { token_srv.cancelled().await })
        .into_future();
    tokio::pin!(server);
    let mut tasks = SupervisedTasks::new(token);
    for task in service_router.background_tasks {
        tasks.adopt(task.name, task.handle);
    }
    if let Some(writer) = security.audit_writer {
        tasks.adopt("audit writer", writer);
    }
    let outcome = tokio::select! {
        result = &mut server => result.map_err(ServiceRuntimeError::from),
        name = tasks.first_unexpected_exit() => {
            // `first_unexpected_exit` has cancelled the token, so the server
            // is already draining. Let it finish before reporting.
            if let Err(error) = server.await {
                tracing::warn!(%error, "HTTP server stopped with an error while draining");
            }
            Err(CriticalTaskError(name).into())
        }
    };
    // The listener has stopped, so no request records an event after this.
    security.audit_stop.cancel();
    tasks.shutdown().await;
    outcome
}

/// The audit context that the handlers of `config.target` record through.
///
/// The handle is the one in `dependencies`, or a disabled handle. A broker ACL
/// refusal names `config.wal_topic`. An ingester operation names the instance
/// that the role's `/ring` page shows. Authentication is required when the
/// injected server security, or else the configured flags, name a credentials
/// file. That is the same test `ServerSecurityArgs::load` applies.
pub(crate) fn service_audit_for_config(
    config: &ServiceConfig,
    dependencies: &ServiceDependencies,
) -> ServiceAudit {
    let ops = match config.target {
        Role::Distributor => DISTRIBUTOR_OPS,
        Role::Querier => QUERIER_OPS,
        Role::BlockBuilder => BLOCK_BUILDER_OPS,
        Role::All => ALL_OPS,
    };
    ServiceAudit {
        handle: dependencies
            .audit
            .clone()
            .unwrap_or_else(AuditHandle::disabled),
        wal_topic: Arc::from(config.wal_topic.as_str()),
        instance: Arc::from(ops.ring_component),
        authentication_required: dependencies.server_security.as_ref().map_or_else(
            || config.server_security.auth_credentials_config.is_some(),
            ServerSecurity::authentication_enabled,
        ),
    }
}

/// Waits for an operating-system signal asking the process to stop.
///
/// `SIGTERM` -- what Kubernetes, systemd and `docker stop` send -- and
/// `SIGINT` -- what Ctrl+C sends -- both resolve the future. Loki, Mimir and
/// Tempo trap the two identically and run the same stop chain on either, and
/// so does every role here: a role that heard only `SIGINT` would be killed at
/// the end of the orchestrator's grace period, losing whatever it had not yet
/// flushed and committed.
///
/// A handler that cannot be installed also resolves the future. A process that
/// cannot hear a stop request should stop rather than run on unstoppable.
///
/// The stack is Unix-only -- the Bazel toolchains list Linux and macOS, CI
/// runs on Linux, and `krabka-pprof` needs Linux -- so this needs no fallback
/// for a platform without `SignalKind`.
pub async fn shutdown_signal() {
    let interrupt = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to install SIGINT handler; triggering shutdown");
        }
    };

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

    tokio::select! {
        () = interrupt => {},
        () = terminate => {},
    }
}

pub(crate) struct AuditedRuntimeSecurity {
    pub(crate) security: RuntimeSecurity,
    pub(crate) stop_guard: DropGuard,
    pub(crate) dependencies: ServiceDependencies,
}

pub(crate) async fn start_audited_runtime_security(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
) -> Result<AuditedRuntimeSecurity, ServiceRuntimeError> {
    let security = start_runtime_security(config, &dependencies).await?;
    let stop_guard = security.audit_stop.clone().drop_guard();
    let dependencies = dependencies.with_audit(security.audit.clone());
    Ok(AuditedRuntimeSecurity {
        security,
        stop_guard,
        dependencies,
    })
}

/// Loads the data-port security of a role and starts its audit layer.
///
/// A security or an audit handle in `dependencies` wins over the flags in
/// `config`. Otherwise this loads `config.server_security`, and it starts the
/// audit layer that `config.audit` names. That layer writes to the WAL
/// bootstrap servers unless `--audit-bootstrap` names others, under the WAL
/// client security of `dependencies`. With no flag set, the data port serves
/// plain HTTP, and the audit layer is disabled and spawns no task.
///
/// Every security event of the listener goes to the audit handle.
///
/// # Errors
///
/// Returns [`ServiceRuntimeError::ServerSecurity`] when the data-port security
/// flags do not load, and [`ServiceRuntimeError::Audit`] when the audit layer
/// does not start.
pub(crate) async fn start_runtime_security(
    config: &ServiceConfig,
    dependencies: &ServiceDependencies,
) -> Result<RuntimeSecurity, ServiceRuntimeError> {
    let server = match &dependencies.server_security {
        Some(security) => security.clone(),
        None => config.server_security.load()?,
    };
    let audit_stop = CancellationToken::new();
    let audit_parts = match &dependencies.audit {
        Some(audit) => AuditServiceParts {
            handle: audit.clone(),
            writer: None,
        },
        None => AuditService::start(
            &config.audit,
            krabka_product(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")),
            config.wal_bootstrap_server.as_deref(),
            dependencies.wal_security.as_ref(),
            audit_stop.clone(),
        )
        .await?
        .into_parts(),
    };
    let audit = audit_parts.handle;
    let audit_writer = audit_parts.writer;
    Ok(RuntimeSecurity {
        server: server.with_security_events(Arc::new(audit.clone())),
        audit,
        audit_writer,
        audit_stop,
    })
}
