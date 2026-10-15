use super::*;
use krabka_observability::compaction_schedule::CompactionSchedule;
use std::future::IntoFuture as _;
use krabka_observability::RoleKind;
use krabka_observability::server_security::ServerListener;
use krabka_observability::server_security::serve_router;
use krabka_traces::frontend::HttpQuerier;
use krabka_traces::frontend::MembershipView;
use krabka_traces::frontend::QuerierScheme;
use krabka_units::{ByteSize, convert::{ByteSizeExt as _, TimeExt as _}};
use tokio::net::TcpListener;
use krabka_observability::RoleReadiness;
use krabka_observability::wal_group_assignment::WalRebalanceListener;
use krabka_observability::SupervisedTasks;
use krabka_units::fmt::Human as _;
use std::time::SystemTime;
use krabka_blockstore::DEFAULT_BLOCK_SWEEP_GRACE;
use krabka_client_consumer::IsolationLevel;

/// Runs compaction passes on `--compaction-interval` until `shutdown` fires.
///
/// Everything a pass needs is owned rather than borrowed, because the role
/// supervises this loop rather than awaiting it inline: a supervised task holds
/// its own state for as long as it runs. See
/// [`run_compactor`](super::run_compactor::run_compactor) for what supervision
/// buys.
pub(crate) async fn compaction_loop(
    cli: Cli,
    configured: ConfiguredObjectStore,
    policy: CompactionPolicy,
    overrides: OverridesProvider,
    metrics: ServiceMetrics,
    shutdown: CancellationToken,
) {
    let mut schedule = CompactionSchedule::new(cli.compaction_interval, shutdown);
    while schedule.next_pass_due().await {
        let outcome = metrics
            .compaction
            .time_run(run_compactor_once(
                &cli,
                &configured,
                policy,
                &overrides,
                &metrics,
            ))
            .await;
        match outcome {
            Ok(compacted_blocks) => {
                tracing::info!(compacted_blocks, "traces compactor finished one pass");
            }
            // One failed pass is not a reason to lose the role. The next tick
            // reloads the index and replans from whatever is durable. The
            // counter is what makes that visible: without it a role whose every
            // pass fails exports what a role with nothing to do exports.
            Err(error) => {
                tracing::warn!(%error, "traces compaction pass failed; retrying on the next tick");
            }
        }
    }
}

/// The query-frontend of `--target all`, fanning out to the querier beside it.
///
/// This is the one role the composition cannot reuse as-is, and the reason is
/// a cycle. [`frontend::run_query_frontend`] discovers its queriers by probing
/// their `/ready`, and registers a `querier-membership` gate that it holds
/// down until one of them answers 200. In a single-process stack that gate is
/// in the *same* [`RoleReadiness`] the querier's own data port reports, so the
/// querier answers 503 naming the frontend's gate, the frontend reads the 503
/// as a querier that is not ready, and neither ever moves. The process would
/// start, bind every port, and fail every query with "no querier is ready",
/// for ever.
///
/// So membership here is [`MembershipView::fixed`] over the one address the
/// composition already knows -- the port the querier's listener actually bound
/// -- and no probe loop runs. Nothing is lost by that: a probe exists to
/// notice queriers appearing and disappearing, and in this process there is
/// exactly one, it is in the same address space, and it cannot outlive the
/// frontend or be replaced under it. What is deliberately *not* skipped is the
/// fan-out itself: [`HttpQuerier`] dials `querier_addr` over real HTTP and
/// speaks the same job protocol it would to a querier in another pod, so the
/// path this target exercises is the deployed one.
///
/// The querier's loopback port serves with the process security, so the
/// fan-out dials it over TLS when the listeners serve TLS, and presents the
/// internal client credential.
///
/// [`RoleReadiness`]: krabka_observability::RoleReadiness
///
/// # Errors
/// Returns an error when the block catalog cannot be built, when the querier
/// transport cannot be constructed, or when the HTTP server fails.
pub(crate) async fn run_all_query_frontend(
    ctx: AllRoleContext,
    listener: TcpListener,
    querier_addr: SocketAddr,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let readiness = ctx.readiness.for_role(RoleKind::QueryFrontend);
    let gates = (ctx.cli.target_bytes_per_job > ByteSize::from_bytes(0))
        .then(|| BlockStoreGates::register(&readiness));
    let security = &ctx.security.server;
    let mut cfg = frontend_config_from_cli(&ctx.cli, listener.local_addr()?)?;
    // `--querier-url` names queriers in other processes. There are none: the
    // only querier this frontend may fan out to is the one in this process,
    // on the port it bound a moment ago.
    cfg.querier_addrs = vec![querier_addr.to_string()];
    cfg.querier_scheme = QuerierScheme::serving_tls(security.tls_enabled());
    let catalog =
        build_trace_index_catalog(&ctx.cli, &ctx.metrics, gates.as_ref(), &ctx.object_store)
            .await?;
    let backend = HttpQuerier::new(
        cfg.request_timeout.to_std(),
        cfg.querier_scheme,
        security.internal_client(),
    )?;
    let membership = MembershipView::fixed(cfg.querier_addrs.clone());
    let app = build_query_frontend_app(
        backend, catalog, cfg, membership, readiness,
    );
    let listener = ServerListener::bind(listener, security)?;
    let bound = listener.local_addr();
    tracing::info!(%bound, %querier_addr, "traces all-in-one Tempo API listening");
    serve_router(listener, app, security)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .into_future()
        .await?;
    Ok(())
}

/// Consumes the traces WAL and writes blocks.
///
/// `object_store` is the process's store, not this role's: under
/// `--target all` the querier reads back exactly what this role writes, and
/// [`SharedObjectStore`] is what makes those the same store rather than two
/// that happen to share a URL.
pub(crate) async fn run_block_builder(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
    object_store: &SharedObjectStore,
    security: &ProcessSecurity,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // The role has no data port, so the admin port is the only place these
    // report. It still needs them: a block builder that has not reached the
    // broker writes nothing, and looks the same as one with an idle topic.
    let wal_consumer_gate = readiness.gate("wal-consumer");
    let wal_catch_up_gate = readiness.gate("wal-catch-up");
    let gates = BlockStoreGates::register(&readiness);
    let promoted_attrs = promoted_attrs_from_cli(&cli)?;
    let rebalance =
        WalRebalanceListener::new(crate::TRACES_WAL_TOPIC).rewinding_fenced_partitions();
    let consumer = wal_consumer(
        &cli,
        "krabka-traces-block-builder",
        None,
        security.wal.as_ref(),
        Some(rebalance.clone()),
    )
    .await?;
    wal_consumer_gate.mark_ready();
    let configured = object_store
        .get(
            &cli,
            metrics.object_store.clone(),
            ObjectStoreAccess::ReadWrite,
        )
        .await?;
    gates.object_store.mark_ready();
    let writer =
        BlockWriter::new(configured.store.clone()).with_metrics(metrics.object_store.clone());
    let object_key_prefix = configured.prefix.to_string();
    let trace_index_key = configured.object_key(&cli.trace_index_key);
    let initial_index = TraceIndex::load_latest_snapshot_or_empty_with_max_bytes(
        &configured.store,
        &trace_index_key,
        cli.index_snapshot_max,
    )
    .await?;
    gates.trace_index.mark_ready();
    let index = Arc::new(Mutex::new(initial_index));
    blockbuilder::run(
        blockbuilder::BlockBuilderConsumer::with_catch_up(
            consumer,
            &metrics.wal_consumer,
            wal_catch_up_gate,
            rebalance,
        ),
        writer,
        index,
        configured.store,
        blockbuilder::BlockBuilderConfig {
            object_key_prefix,
            index_key: trace_index_key,
            window: cli.block_builder_window,
            empty_poll_backoff: cli.block_builder_empty_poll_backoff,
            promoted_attrs,
            flush_max_records: cli.block_builder_flush_max_records,
            flush_max_age: cli.block_builder_flush_max_age,
            index_snapshot_retain: cli.index_snapshot_retain,
        },
        metrics,
        shutdown,
    )
    .await?;
    Ok(())
}

/// Runs compaction passes on a schedule until shutdown.
///
/// Compaction is not something an operator types a time window into: the
/// planner decides what merges from the levels and time ranges the index
/// already records, and a pass that plans nothing costs one index load.
///
/// Retention is the one thing here an operator does configure, per tenant, so
/// the role builds the [`OverridesProvider`] the sweep reads its windows from.
///
/// The schedule is [`compaction_loop`], and it is a supervised task rather than
/// the body of this function. The loop is the whole role, so a loop that
/// panicked, or that returned while the process was still up, would leave a
/// compactor that keeps its readiness gate and its `/metrics` endpoint and
/// compacts nothing. Neither probe can tell that from a compactor with nothing
/// to do. [`SupervisedTasks`] names the loop, reports either stop as a
/// [`CriticalTaskError`], and drains it on the way out.
///
/// [`OverridesProvider`]: krabka_traces::limits::OverridesProvider
pub(crate) async fn run_compactor(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
    object_store: &SharedObjectStore,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // The one thing a compactor cannot work without. Each pass loads the index
    // itself, so there is no index gate to hold: a pass that cannot read one
    // fails and the next tick retries.
    let object_store_gate = readiness.gate("object-store");
    // Built before the store is reached, so that a malformed overrides file
    // stops the role at once rather than after its first pass has already
    // deleted blocks under the wrong window.
    let overrides = load_traces_limits_overrides_config(
        cli.traces_limits_overrides_config.as_deref(),
        cli.traces_api_overrides_file.as_deref(),
        limits_from_cli(&cli),
    )?;
    let configured = object_store
        .get(
            &cli,
            metrics.object_store.clone(),
            ObjectStoreAccess::ReadWrite,
        )
        .await?;
    object_store_gate.mark_ready();
    let policy = compaction_policy_from_cli(&cli);
    tracing::info!(
        interval = %cli.compaction_interval.human(),
        max_blocks_per_job = policy.max_blocks_per_job(),
        target_rows = policy.target_rows_per_block(),
        max_level = %policy.max_level(),
        default_block_retention = %cli.block_retention.human(),
        "traces compactor scheduling passes"
    );
    let mut tasks = SupervisedTasks::new(shutdown.clone());
    tasks.spawn(
        "traces compactor pass scheduler",
        compaction_loop(
            cli,
            configured,
            policy,
            overrides,
            metrics,
            shutdown.clone(),
        ),
    );
    // A pass already under way finishes before this returns, so `--target all`
    // stops this stage and moves to the next one rather than leaving a loop
    // running behind the drain.
    tasks.run_until_shutdown().await?;
    Ok(())
}

/// Loads the index, compacts what the policy asks for, expires what retention
/// asks for, publishes the result, and then deletes what the index no longer
/// names. Returns how many replacement blocks the pass wrote.
///
/// The order is what keeps a querier honest. The index is the only record of
/// which blocks are live, so a block leaves the index and the snapshot is saved
/// **before** its object is deleted. If the object went first, a reader that
/// still holds the older snapshot would resolve a key whose object is already
/// gone. This order costs only that an object outlives its index entry until
/// the delete lands, and the orphan sweep collects whatever a torn pass left.
///
/// The index is reloaded per pass rather than carried across ticks: the block
/// builder publishes new blocks into the same snapshot chain, and a stale
/// in-memory copy would plan against blocks that have since been replaced.
pub(crate) async fn run_compactor_once(
    cli: &Cli,
    configured: &ConfiguredObjectStore,
    policy: CompactionPolicy,
    overrides: &OverridesProvider,
    metrics: &ServiceMetrics,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let writer =
        BlockWriter::new(configured.store.clone()).with_metrics(metrics.object_store.clone());
    let trace_index_key = configured.object_key(&cli.trace_index_key);
    let mut index = TraceIndex::load_latest_snapshot_or_empty_with_max_bytes(
        &configured.store,
        &trace_index_key,
        cli.index_snapshot_max,
    )
    .await?;
    let now = SystemTime::now();
    let pass = compact_once_with_policy(
        configured.store.clone(),
        &writer,
        &mut index,
        configured.prefix.as_ref(),
        policy,
        cli.block_read_max,
    )
    .await?;
    // A deployment where every window is zero has nothing that can expire, and
    // the plan reads every block of every tenant out of the index to decide it.
    let expired = if overrides.expires_any_blocks() {
        expire_trace_blocks(&mut index, now_unix_nanos(now), overrides)
    } else {
        Vec::new()
    };
    let mut retired = pass.retired_inputs;
    retired.extend(expired.iter().map(|block| block.object_key.clone()));

    // A pass that changed nothing has nothing to publish. A save anyway burns a
    // snapshot generation every tick and evicts the retained history a reader
    // falls back on. The save is also what makes the retired objects safe to
    // delete, so both happen together or neither does.
    if !retired.is_empty() || !pass.outputs.is_empty() {
        metrics.compaction.record_output(pass.outputs.len() as u64);
        index
            .save_latest_snapshot_with_retain(
                &configured.store,
                &trace_index_key,
                cli.index_snapshot_retain,
            )
            .await?;
        let report = delete_trace_blocks(&configured.store, &retired).await;
        metrics.compaction.record_deleted(
            report.blocks_deleted as u64,
            report.sidecars_deleted as u64,
            report.failures.len() as u64,
        );
        if report.failures.is_empty() {
            tracing::debug!(
                blocks_deleted = report.blocks_deleted,
                objects_absent = report.objects_absent,
                expired = expired.len(),
                "traces compactor deleted the blocks its index no longer names"
            );
        } else {
            // Nothing is lost: the blocks are already out of the index, and the
            // orphan sweep reaches them once they are older than its grace.
            tracing::warn!(
                blocks_deleted = report.blocks_deleted,
                failures = report.failures.len(),
                "some retired traces blocks would not delete"
            );
        }
    }

    let swept = sweep_orphaned_trace_blocks(
        &configured.store,
        configured.prefix.as_ref(),
        &trace_index_key,
        &index,
        DEFAULT_BLOCK_SWEEP_GRACE,
        now,
    )
    .await?;
    metrics
        .compaction
        .record_orphan_sweep(swept.deleted as u64, swept.failed as u64);
    tracing::debug!(
        listed = swept.listed,
        live = swept.live,
        kept_within_grace = swept.kept_within_grace,
        deleted = swept.deleted,
        failed = swept.failed,
        "traces compactor reconciled the block prefix against its index"
    );
    Ok(pass.outputs.len())
}

/// Accepts pushes on every ingest protocol traces speaks, and writes the WAL.
///
/// `primary_listen` is [`PrimaryListen::Skip`] only under `--target all`, where `--listen`
/// is the query-frontend's Tempo API port and cannot also be the
/// distributor's. Nothing is lost by dropping it there: the primary listener
/// serves [`distributor::serve`]'s router, and `--otlp-http-listen` serves the
/// *same* router from the same state, so every route reachable on `--listen`
/// is reachable on 4318. The six protocol ports below are unaffected, because
/// those are the ports a collector is configured with.
///
/// Every TCP listener serves with `security.server`, and the WAL producer
/// connects with `security.wal`. The Jaeger compact UDP receiver does not
/// start when authentication is on, because a datagram cannot carry a
/// credential.
pub(crate) async fn run_distributor(
    role: DistributorRole<'_>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let DistributorRole {
        cli,
        metrics,
        readiness,
        shutdown,
        primary_listen,
        security,
    } = role;
    // Nowhere to put a push until the WAL producer has a broker, and the seven
    // ingest listeners below all bind after it. The admin port is already up,
    // so `/ready` reports the connect.
    let wal_broker = readiness.gate("wal-broker");
    // Built before the producer, so that a malformed overrides file stops the
    // role at once rather than after it has connected and bound seven ports.
    let overrides = load_traces_limits_overrides_config(
        cli.traces_limits_overrides_config.as_deref(),
        cli.traces_api_overrides_file.as_deref(),
        limits_from_cli(&cli),
    )?;
    // Boxed: the producer-startup future is several KB and would otherwise be
    // inlined into this role's future (and from there into `run`'s). One
    // allocation at startup keeps the role futures small.
    let producer = Box::pin(
        Producer::builder()
            .bootstrap(cli.bootstrap)
            .dispatch_queue_capacity(cli.client_dispatch_queue_capacity)
            .frame_max(cli.client_frame_max)
            .maybe_security(security.wal.clone())
            .build(),
    )
    .await?;
    wal_broker.mark_ready();
    let mut state =
        DistributorState::with_metrics(Arc::new(KafkaSink::new(Arc::new(producer))), metrics);
    state.overrides = overrides;
    state.max_decompressed = cli.max_decompressed_bytes;
    let state = Arc::new(state);
    let grpc_addr: SocketAddr = cli.grpc_listen.parse()?;
    let otlp_http_addr: SocketAddr = cli.otlp_http_listen.parse()?;
    let jaeger_grpc_addr: SocketAddr = cli.jaeger_grpc_listen.parse()?;
    let jaeger_compact_addr: SocketAddr = cli.jaeger_compact_listen.parse()?;
    let jaeger_http_addr: SocketAddr = cli.jaeger_http_listen.parse()?;
    let zipkin_addr: SocketAddr = cli.zipkin_listen.parse()?;
    let server = &security.server;
    let endpoint = |addr: SocketAddr| distributor::ReceiverEndpoint {
        addr,
        state: Arc::clone(&state),
        security: server,
        shutdown: shutdown.clone(),
    };

    // Seven listeners, one role. A distributor that has lost one of them still
    // binds the other six and still passes a liveness probe, while everything
    // pushed to the lost port is dropped. Each accept loop is therefore
    // supervised by name, and any of them ending -- including by a panic,
    // which no `if let Err` in the task body could have seen -- ends the role.
    let mut tasks = SupervisedTasks::new(shutdown.clone());

    let grpc = distributor::serve_otlp_grpc(endpoint(grpc_addr)).await?;
    tasks.adopt("traces distributor OTLP/gRPC", grpc.task);
    tracing::info!(bound = %grpc.bound_addr, "traces distributor OTLP/gRPC listening");
    let jaeger_grpc = distributor::serve_jaeger_grpc(endpoint(jaeger_grpc_addr)).await?;
    tasks.adopt("traces distributor Jaeger gRPC", jaeger_grpc.task);
    tracing::info!(bound = %jaeger_grpc.bound_addr, "traces distributor Jaeger gRPC listening");
    if let Some(jaeger_compact) =
        distributor::serve_jaeger_compact_udp(endpoint(jaeger_compact_addr)).await?
    {
        tasks.adopt("traces distributor Jaeger compact UDP", jaeger_compact.task);
        tracing::info!(bound = %jaeger_compact.bound_addr, "traces distributor Jaeger compact UDP listening");
    }
    let otlp_http = distributor::serve(endpoint(otlp_http_addr)).await?;
    tasks.adopt("traces distributor OTLP/HTTP", otlp_http.task);
    tracing::info!(bound = %otlp_http.bound_addr, "traces distributor OTLP/HTTP listening");
    let jaeger_http = distributor::serve(endpoint(jaeger_http_addr)).await?;
    tasks.adopt("traces distributor Jaeger thrift HTTP", jaeger_http.task);
    tracing::info!(bound = %jaeger_http.bound_addr, "traces distributor Jaeger thrift HTTP listening");
    let zipkin = distributor::serve(endpoint(zipkin_addr)).await?;
    tasks.adopt("traces distributor Zipkin HTTP", zipkin.task);
    tracing::info!(bound = %zipkin.bound_addr, "traces distributor Zipkin HTTP listening");
    if primary_listen == PrimaryListen::Serve {
        let addr: SocketAddr = cli.listen.parse()?;
        let primary = distributor::serve(endpoint(addr)).await?;
        tasks.adopt("traces distributor HTTP", primary.task);
        tracing::info!(bound = %primary.bound_addr, "traces distributor listening");
    }

    tasks.run_until_shutdown().await?;
    Ok(())
}

/// Holds the window of spans no block covers yet, and answers over it.
///
/// `listener` is already bound, for the same reason the querier's is: under
/// `--target all` this role takes an ephemeral loopback port and the querier
/// in the same process is pointed at whichever one it got.
pub(crate) async fn run_live_store(
    role: ListeningRole<'_>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let ListeningRole {
        cli,
        metrics,
        readiness,
        shutdown,
        listener,
        security,
    } = role;
    // The consumer is the live tier. Without it the role keeps its port and
    // answers every search from a store that stopped at the last record it
    // read, and nothing in the answer says so. The gate is registered before
    // the connect and goes back down when the loop ends.
    let wal_consumer_gate = readiness.gate("wal-consumer");
    let wal_catch_up_gate = readiness.gate("wal-catch-up");
    let consumer = wal_consumer(
        &cli,
        "krabka-traces-live-store",
        None,
        security.wal.as_ref(),
        None,
    )
    .await?;
    let store = Arc::new(RwLock::new(LiveStore::new(cli.retention.nanos_i64())));
    let router = build_live_store_router(&cli, Arc::clone(&store), readiness)?;
    let mut tasks = SupervisedTasks::new(shutdown.clone());
    let live_shutdown = shutdown.clone();
    tasks.spawn("traces live-store consumer", async move {
        wal_consumer_gate.mark_ready();
        if let Err(err) =
            livestore::run(consumer, store, metrics, live_shutdown, wal_catch_up_gate).await
        {
            tracing::error!(error = %err, "traces live-store consumer stopped");
        }
        wal_consumer_gate.mark_unready();
    });

    serve_role_router(RoleServer {
        role: "traces live-store",
        listener,
        router,
        security,
        tasks,
        shutdown,
    })
    .await
}

pub(crate) async fn run_metrics_generator(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
    security: &ProcessSecurity,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // The spans this role reads all come from the WAL. Until the consumer has
    // a broker it generates no metrics at all, and the admin port says so.
    let wal_consumer_gate = readiness.gate("wal-consumer");
    let catch_up = readiness.gate("wal-catch-up");
    let mut cfg = if let Some(path) = &cli.config {
        let bytes = std::fs::read_to_string(path)?;
        serde_yaml::from_str::<MetricsGenConfig>(&bytes)?
    } else {
        MetricsGenConfig::default()
    };
    if let Some(path) = &cli.traces_limits_overrides_config {
        cfg.apply_runtime_overrides(&std::fs::read_to_string(path)?)?;
    }
    apply_metrics_generator_cli_overrides(&mut cfg, &cli);

    let consumer = wal_consumer(
        &cli,
        "krabka-traces-metrics-generator",
        None,
        security.wal.as_ref(),
        None,
    )
    .await?;
    wal_consumer_gate.mark_ready();
    let source = Arc::new(KafkaSpanSource::new(
        consumer,
        metrics.wal_consumer,
        catch_up,
    ));
    // The remote-write target is a Krabka metrics distributor, so the sink
    // presents the internal client credential.
    let sink = Arc::new(PrometheusRemoteWriteSink::new(
        cfg.remote_write_url.clone(),
        security.server.internal_client(),
    )?);
    let service = MetricsGenService::new(cfg, Arc::new(SystemClock), source, sink)
        .with_poll_policy(
            cli.metrics_generator_poll_batch_size,
            cli.metrics_generator_poll_error_backoff,
        );
    service.run(shutdown).await?;
    Ok(())
}

pub(crate) async fn run_query_frontend(
    cli: Cli,
    metrics: ServiceMetrics,
    readiness: RoleReadiness,
    shutdown: CancellationToken,
    object_store: &SharedObjectStore,
    security: &ProcessSecurity,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = cli.listen.parse()?;
    let mut cfg = frontend_config_from_cli(&cli, addr)?;
    cfg.overrides = load_traces_limits_overrides_config(
        cli.traces_limits_overrides_config.as_deref(),
        cli.traces_api_overrides_file.as_deref(),
        limits_from_cli(&cli),
    )?;
    // The catalog is built before the listener binds; `querier-membership` is
    // the gate that keeps moving after it, and `frontend::run_query_frontend`
    // registers that one.
    let gates = (cli.target_bytes_per_job > ByteSize::from_bytes(0))
        .then(|| BlockStoreGates::register(&readiness));
    let catalog = build_trace_index_catalog(&cli, &metrics, gates.as_ref(), object_store).await?;
    tracing::info!(%addr, "traces query-frontend listening");
    frontend::run_query_frontend(cfg, catalog, readiness, &security.server, shutdown).await?;
    Ok(())
}

/// A consumer of the traces WAL in `group_id`, with the fetch and client
/// limits of `cli`, that connects to the broker with `security`.
pub(crate) async fn wal_consumer(
    cli: &Cli,
    group_id: &str,
    group_instance_id: Option<&str>,
    security: Option<&ClientSecurity>,
    rebalance: Option<WalRebalanceListener>,
) -> Result<Consumer, krabka_client_consumer::ConsumerError> {
    // Boxed: consumer startup (bootstrap resolve, double `JoinGroup`,
    // `SyncGroup`, offset priming) builds a ~13 KB future. Every role that
    // reads the WAL awaits this, so leaving it inline pushes each role future
    // — and the `run` dispatcher that unions them — past `clippy::large_futures`.
    // The consumer is built once per process, so the allocation is free.
    Box::pin(
        Consumer::builder()
            .bootstrap(cli.bootstrap.clone())
            .group_id(group_id.to_string())
            .maybe_group_instance_id(group_instance_id)
            .fetch_max(cli.wal_fetch_max)
            .fetch_partition_max(cli.wal_fetch_partition_max)
            .dispatch_queue_capacity(cli.client_dispatch_queue_capacity)
            .frame_max(cli.client_frame_max)
            .maybe_security(security.cloned())
            .subscribe(vec![TRACES_WAL_TOPIC.to_string()])
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .isolation_level(IsolationLevel::ReadCommitted)
            .maybe_rebalance_listener(rebalance.map(|listener| {
                Box::new(listener) as Box<dyn krabka_client_consumer::ConsumerRebalanceListener>
            }))
            .enable_auto_commit(false)
            .build(),
    )
    .await
}
