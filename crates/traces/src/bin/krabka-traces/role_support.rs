use std::collections::BTreeMap;

use krabka_observability::{CancellationToken, ReadinessGate, RoleKind, RoleReadiness};
use tokio::net::TcpListener;

use super::{
    AllRoleStage, Args, Cli, ClientSecurity, DistributorRole, PrimaryListen, ServerSecurity,
    ServiceMetrics, SharedObjectStore, ValueEnum, all_role_stage, run_all_query_frontend,
    run_block_builder, run_compactor, run_distributor, run_live_store, run_metrics_generator,
    run_querier,
};

/// The five things every role of `--target all` needs, and one of each.
///
/// Each field is shared rather than per-role, and each for its own reason:
///
/// * `cli` is the process's configuration. A role that wants a different value
///   -- the querier's `--querier-live-store-url`, which only the composition
///   knows -- clones this and edits its own copy, so the edit is visible where
///   it is made instead of being a flag the operator has to set.
/// * `metrics` is one registry, because the admin port exports one `/metrics`.
/// * `readiness` is one gate list, so `/ready` is one answer for the process.
///   Roles take [`RoleReadiness::for_role`] views of it, which stamp the role's
///   name onto the gates they register: the probe then says
///   `not ready: block-builder/wal-consumer` rather than a bare `wal-consumer`
///   that four of the roles could have registered.
/// * `object_store` is one store. See [`SharedObjectStore`] for what four of
///   them would do to a `memory:///` deployment.
/// * `security` is one loaded posture, so every listener serves alike and one
///   audit trail records what all of them report. See
///   [`run_all`](super::run_all::run_all) for what that means for the
///   loopback ports.
#[derive(Clone)]
pub(crate) struct AllRoleContext {
    pub(crate) cli: Cli,
    pub(crate) metrics: ServiceMetrics,
    pub(crate) readiness: RoleReadiness,
    pub(crate) object_store: SharedObjectStore,
    pub(crate) security: ProcessSecurity,
}

/// Every role `--target all` runs, keyed by name and not yet started.
///
/// The three listeners are already bound, and which port each one is on is
/// half of what this function decides:
///
/// * `frontend` is `--listen`. That is the port a Grafana Tempo datasource is
///   given, and the query-frontend is what answers it, because the frontend is
///   the front of the read path in every other deployment too.
/// * `querier` and `live_store` are ephemeral loopback ports. Nothing outside
///   the process has business dialling them, and pinning them to fixed numbers
///   would make two all-in-one processes on one machine collide over ports
///   neither of their operators chose. Their addresses are read back from the
///   listeners rather than guessed, and handed to the roles that dial them:
///   the frontend fans out to `querier` over real HTTP, and the querier reaches
///   `live_store` through `--querier-live-store-url`, so both of those paths
///   are the ones a distributed deployment runs rather than an in-process
///   shortcut.
///
/// # Errors
/// Returns an error when a bound listener cannot report its own address.
pub(crate) fn all_role_stages(
    ctx: &AllRoleContext,
    frontend: TcpListener,
    querier: TcpListener,
    live_store: TcpListener,
) -> Result<BTreeMap<RoleKind, AllRoleStage>, Box<dyn std::error::Error + Send + Sync>> {
    let querier_addr = querier.local_addr()?;
    let live_store_addr = live_store.local_addr()?;
    let mut stages = BTreeMap::new();

    let distributor = ctx.clone();
    stages.insert(
        RoleKind::Distributor,
        all_role_stage(move |token| async move {
            log_role_outcome(
                RoleKind::Distributor,
                run_distributor(DistributorRole {
                    cli: distributor.cli,
                    metrics: distributor.metrics,
                    readiness: distributor.readiness.for_role(RoleKind::Distributor),
                    shutdown: token,
                    primary_listen: PrimaryListen::Skip,
                    security: &distributor.security,
                })
                .await,
            );
        }),
    );

    let mut builder = ctx.clone();
    builder.metrics.wal_consumer = builder.metrics.wal_consumer.with_fresh_recovery();
    builder
        .readiness
        .track_wal_consumer(builder.metrics.wal_consumer.clone());
    stages.insert(
        RoleKind::BlockBuilder,
        all_role_stage(move |token| async move {
            log_role_outcome(
                RoleKind::BlockBuilder,
                run_block_builder(
                    builder.cli,
                    builder.metrics,
                    builder.readiness.for_role(RoleKind::BlockBuilder),
                    token,
                    &builder.object_store,
                    &builder.security,
                )
                .await,
            );
        }),
    );

    let mut live = ctx.clone();
    live.metrics.wal_consumer = live.metrics.wal_consumer.with_fresh_recovery();
    live.readiness
        .track_wal_consumer(live.metrics.wal_consumer.clone());
    stages.insert(
        RoleKind::LiveStore,
        all_role_stage(move |token| async move {
            log_role_outcome(
                RoleKind::LiveStore,
                run_live_store(ListeningRole {
                    cli: live.cli,
                    metrics: live.metrics,
                    readiness: live.readiness.for_role(RoleKind::LiveStore),
                    shutdown: token,
                    listener: live_store,
                    security: &live.security,
                })
                .await,
            );
        }),
    );

    let mut reader = ctx.clone();
    // The querier reads the recent window from the live-store role over HTTP
    // rather than from a live tier of its own. Both would answer, but only one
    // of them is the live-store actually running: a querier with
    // `--querier-live-store` set consumes the WAL itself, and `--target all`
    // would then be six roles and a copy of the seventh.
    reader.cli.querier_live_store = false;
    // The live-store port serves with the process security, so the URL names
    // TLS when the listeners serve it.
    let live_store_scheme = if ctx.security.server.tls_enabled() {
        "https"
    } else {
        "http"
    };
    reader.cli.querier_live_store_url = Some(format!("{live_store_scheme}://{live_store_addr}"));
    stages.insert(
        RoleKind::Querier,
        all_role_stage(move |token| async move {
            log_role_outcome(
                RoleKind::Querier,
                run_querier(
                    ListeningRole {
                        cli: reader.cli,
                        metrics: reader.metrics,
                        readiness: reader.readiness.for_role(RoleKind::Querier),
                        shutdown: token,
                        listener: querier,
                        security: &reader.security,
                    },
                    &reader.object_store,
                )
                .await,
            );
        }),
    );

    let front = ctx.clone();
    stages.insert(
        RoleKind::QueryFrontend,
        all_role_stage(move |token| async move {
            log_role_outcome(
                RoleKind::QueryFrontend,
                run_all_query_frontend(front, frontend, querier_addr, token).await,
            );
        }),
    );

    let compactor = ctx.clone();
    stages.insert(
        RoleKind::Compactor,
        all_role_stage(move |token| async move {
            log_role_outcome(
                RoleKind::Compactor,
                run_compactor(
                    compactor.cli,
                    compactor.metrics,
                    compactor.readiness.for_role(RoleKind::Compactor),
                    token,
                    &compactor.object_store,
                )
                .await,
            );
        }),
    );

    // Tempo's `-target=all` includes the metrics-generator unconditionally.
    // This one does not, and the reason is in `MetricsGenConfig::default`: its
    // `remote_write_url` is `http://localhost:9009/api/v1/push`, Mimir's push
    // endpoint, which on the single machine this target exists for is nothing
    // at all. `MetricsGenService::run` logs `metrics-generator flush failed`
    // once per collection interval, for ever, and the first thing an operator
    // trying the stack would see is a role failing at a URL they never chose.
    // Tempo gets away with it because its `all` is a demo of the whole Grafana
    // stack, Mimir included. So: the role joins the composition when the
    // operator has said where the metrics go, by `--remote-write-url` or by a
    // `--config` file, and otherwise says once that it is not running.
    let mut generator = ctx.clone();
    if generator.cli.remote_write_url.is_some() || generator.cli.config.is_some() {
        generator.metrics.wal_consumer = generator.metrics.wal_consumer.with_fresh_recovery();
        generator
            .readiness
            .track_wal_consumer(generator.metrics.wal_consumer.clone());
        stages.insert(
            RoleKind::MetricsGenerator,
            all_role_stage(move |token| async move {
                log_role_outcome(
                    RoleKind::MetricsGenerator,
                    run_metrics_generator(
                        generator.cli,
                        generator.metrics,
                        generator.readiness.for_role(RoleKind::MetricsGenerator),
                        token,
                        &generator.security,
                    )
                    .await,
                );
            }),
        );
    } else {
        tracing::info!(
            "traces all-in-one is not running the metrics-generator: no --remote-write-url \
             and no --config, so it would have nowhere to send what it derives"
        );
    }

    Ok(stages)
}

/// The two preconditions every role that reads blocks has to clear.
///
/// Both are registered before the work starts, so the admin port reports them
/// for the whole window between its own bind and the end of the start. A role
/// that registered a gate only after satisfying it would answer `ready` during
/// exactly the window the gate exists to cover.
pub(crate) struct BlockStoreGates {
    /// Met when the object store is configured.
    pub(crate) object_store: ReadinessGate,
    /// Met when the trace index snapshot is loaded.
    ///
    /// A querier that answers before this holds returns an empty result rather
    /// than an error, and the query-frontend in front of it cannot tell the two
    /// apart.
    pub(crate) trace_index: ReadinessGate,
}

impl BlockStoreGates {
    /// Registers both gates, unmet, in the order the start meets them.
    pub(crate) fn register(readiness: &RoleReadiness) -> Self {
        Self {
            object_store: readiness.gate("object-store"),
            trace_index: readiness.gate("trace-index"),
        }
    }
}

/// A role that serves on a listener its caller has already bound.
///
/// The caller binds `listener` because under `--target all` the role takes an
/// ephemeral loopback port, and another role in the same process has to be
/// told which one it got.
pub(crate) struct ListeningRole<'a> {
    pub(crate) cli: Cli,
    pub(crate) metrics: ServiceMetrics,
    pub(crate) readiness: RoleReadiness,
    pub(crate) shutdown: CancellationToken,
    pub(crate) listener: tokio::net::TcpListener,
    pub(crate) security: &'a ProcessSecurity,
}

/// Records why a role of `--target all` stopped.
///
/// Every role in the composition is boxed down to a future that returns
/// nothing, so this is the last place its error exists. The process still
/// notices the stop -- `StagedDrain` reports the stage by name and the exit
/// code follows from that -- but without this line the log would say a role is
/// gone and never say what it said on the way out.
pub(crate) fn log_role_outcome(
    role: RoleKind,
    outcome: Result<(), Box<dyn std::error::Error + Send + Sync>>,
) {
    match outcome {
        Ok(()) => tracing::info!(role = role.as_str(), "traces role stopped"),
        Err(error) => tracing::error!(role = role.as_str(), %error, "traces role stopped"),
    }
}

#[derive(Args, Clone, Debug)]
pub(crate) struct MetricsFlags {
    #[arg(long, env = "KRABKA_TRACES_ENABLE_TARGET_INFO")]
    pub(crate) enable_target_info: bool,
    #[arg(long, env = "KRABKA_TRACES_ENABLE_STATUS_MESSAGE")]
    pub(crate) enable_status_message: bool,
    #[arg(long, env = "KRABKA_TRACES_ENABLE_MESSAGING_SYSTEM_LATENCY")]
    pub(crate) enable_messaging_system_latency: bool,
}

/// The loaded security of one `krabka-traces` process.
///
/// [`run`](super::run::run) loads it once from the flags, and every role takes the
/// same value. Each data and ingest listener serves with `server`, and each
/// WAL producer and consumer connects with `wal`.
///
/// The struct has no `Debug`. `wal` holds the SASL password, and
/// `krabka-client-core` prints it under `{:?}`.
#[derive(Clone, Default)]
pub(crate) struct ProcessSecurity {
    /// TLS, authentication, the security-event sink, and the internal client.
    pub(crate) server: ServerSecurity,
    /// The broker connection policy. `None` connects in plain text.
    pub(crate) wal: Option<ClientSecurity>,
}

/// Refuses `--target all` with authentication on and no internal client.
///
/// Under `--target all` the query-frontend calls the querier, and the querier
/// calls the live-store, on loopback ports that serve with the same security
/// as every other listener. With authentication on and no
/// `--internal-client-*` flag, each of those calls is a 401, and every query
/// fails. The process stops at start instead.
///
/// # Errors
/// Returns an error that names the missing flags when `security` turns on
/// authentication and configures no internal client.
pub(crate) fn require_internal_credential(
    security: &ServerSecurity,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if security.authentication_enabled() && !security.internal_client().is_configured() {
        return Err("--target all with --auth-credentials-config needs --internal-client-token-path or --internal-client-tls-cert-path: the query-frontend and the querier call the roles beside them through authenticated listeners".into());
    }
    Ok(())
}

/// The roles `krabka-traces` has.
///
/// Traces carries two stages the other signals do not, and both are real
/// differences rather than accidents of this binary. [`LiveStore`] exists
/// because traces is the only signal with a separate hot tier: a logs or
/// metrics querier tails the WAL itself for the window no block covers yet,
/// whereas a trace has to be assembled from spans that arrive over seconds and
/// is therefore held, and queried, in a role of its own.
/// [`MetricsGenerator`] exists because traces is the only signal that derives
/// another signal from itself -- span metrics and a service graph, remote-
/// written to a Prometheus endpoint.
///
/// The spellings are not this repository's to choose. Tempo 3.0 and later
/// names these stages `block-builder`, `live-store`, `metrics-generator`,
/// `querier`, `query-frontend`, `distributor` and `all`, and an operator
/// arriving from Tempo types those. The one name Tempo does not supply is
/// [`Compactor`]: Mimir's word for the job that merges blocks already in
/// object storage into larger ones, which is exactly what this crate's
/// compactor does and is why it carries Mimir's name for it.
///
/// [`LiveStore`]: Self::LiveStore
/// [`MetricsGenerator`]: Self::MetricsGenerator
/// [`Compactor`]: Self::Compactor
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub(crate) enum Target {
    Distributor,
    BlockBuilder,
    LiveStore,
    Querier,
    QueryFrontend,
    Compactor,
    MetricsGenerator,
    /// Every role above, in one process. See
    /// [`run_all`](super::run_all::run_all) for what it composes and in what
    /// order it stops.
    All,
}

impl Target {
    /// The cross-signal name of this role.
    ///
    /// This enum is the subset of the stack's stages that traces implements,
    /// and it exists because clap needs an enum to parse `--target` into.
    /// [`RoleKind`] is the vocabulary itself: it spans all four signals, and
    /// its `as_str` is the one spelling an operator, a manifest and a
    /// readiness gate all use. Mapping one onto the other here -- rather than
    /// letting two independent kebab-case derives happen to agree -- is what
    /// keeps a rename of a stage from silently forking into a traces spelling
    /// and an everything-else spelling.
    pub(crate) const fn kind(self) -> RoleKind {
        match self {
            Self::Distributor => RoleKind::Distributor,
            Self::BlockBuilder => RoleKind::BlockBuilder,
            Self::LiveStore => RoleKind::LiveStore,
            Self::Querier => RoleKind::Querier,
            Self::QueryFrontend => RoleKind::QueryFrontend,
            Self::Compactor => RoleKind::Compactor,
            Self::MetricsGenerator => RoleKind::MetricsGenerator,
            Self::All => RoleKind::All,
        }
    }
}
