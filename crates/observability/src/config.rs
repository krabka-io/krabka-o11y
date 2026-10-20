use crate::{
    AdminError, BlockStoreError, ByteSize, CompactionFrontierStoreError, CompactorRunError,
    ConsumerError, CriticalTaskError, Error, LogDeleteRequestStoreError, LokiRuleStoreError,
    NonZeroUsize, OverridesError, Parser, PathBuf, ProducerError, RoleKind, SocketAddr, Time,
    ValueEnum,
    audit::{AuditArgs, AuditBuildError},
    days, millis, minutes, secs,
    server_security::{ServerSecurityArgs, ServerSecurityError},
    wal_client_security::{WalClientSecurityArgs, WalClientSecurityError},
};
mod service_config;
pub use service_config::ServiceConfig;

/// `Loki`'s `reject_old_samples_max_age` default: samples older than this are
/// refused on ingest.
pub(crate) const LOKI_REJECT_OLD_SAMPLES_MAX_AGE: Time = days(7);

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum QuerierIndexSource {
    LocalManifest,
    TenantObjectStoreManifest,
    TenantObjectStoreShards,
}

/// The roles `krabka-observability` can run.
///
/// Logs has three of them, and the two stages it does not have are absent for
/// reasons rather than by oversight. There is no `live-store`: a logs querier
/// tails the WAL itself, so the recent window is served by the same role that
/// serves the blocks. There is no separate `compactor` either -- `Loki` folds
/// index compaction, retention and delete-request materialisation into the
/// stage that writes durable storage, and so does
/// [`BlockBuilder`](Self::BlockBuilder).
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum Role {
    /// Accepts `Loki` and OTLP pushes and writes the logs WAL.
    Distributor,
    /// Consumes the logs WAL, writes blocks to object storage, and
    /// materialises delete requests as it goes.
    BlockBuilder,
    /// Serves the `Loki` query API over the blocks and the WAL tail.
    Querier,
    /// All three, in one process.
    All,
}

impl Role {
    /// This role in the vocabulary every signal shares.
    #[must_use]
    pub const fn kind(self) -> RoleKind {
        match self {
            Self::Distributor => RoleKind::Distributor,
            Self::BlockBuilder => RoleKind::BlockBuilder,
            Self::Querier => RoleKind::Querier,
            Self::All => RoleKind::All,
        }
    }
}

#[derive(Debug, Error)]
pub enum ServiceConfigError {
    #[error("WAL connect attempt timeout must not exceed startup deadline")]
    WalConnectAttemptExceedsDeadline,
    #[error("WAL connect initial backoff must not exceed maximum backoff")]
    WalConnectInitialBackoffExceedsMaximum,
    #[error("compactor accumulation poll timeout must not exceed accumulation window")]
    CompactorAccumulationPollExceedsWindow,
    #[error("compactor object-store initial backoff must not exceed maximum backoff")]
    CompactorObjectStoreInitialBackoffExceedsMaximum,
    #[error("broker access max staleness must not be shorter than the broker access cache TTL")]
    BrokerAccessStalenessBelowTtl,
    #[error("WAL sink is required for distributor service startup")]
    MissingWalSink,
    #[error("WAL consumer is required for compactor service startup")]
    MissingWalConsumer,
    #[error("missing --wal-bootstrap-server for WAL-backed service startup")]
    MissingWalBootstrapServer,
    #[error("object store is required for object-store querier index sources")]
    MissingObjectStore,
    #[error("missing --index-prefix for compactor service startup")]
    MissingCompactorIndexPrefix,
    #[error("missing --tenant for querier index source {index_source:?}")]
    MissingTenant { index_source: QuerierIndexSource },
    #[error("missing --index-prefix for querier index source {index_source:?}")]
    MissingIndexPrefix { index_source: QuerierIndexSource },
    #[error("missing --query-start-ns for querier index source tenant-object-store-shards")]
    MissingQueryStartNs,
    #[error("missing --query-end-ns for querier index source tenant-object-store-shards")]
    MissingQueryEndNs,
    #[error("invalid --object-store-url {url}: {reason}")]
    InvalidObjectStoreUrl { url: String, reason: String },
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error(transparent)]
    Frontier(#[from] CompactionFrontierStoreError),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    ObjectStoreSemantics(#[from] krabka_blockstore::ObjectStoreSemanticsError),
    #[error(transparent)]
    DeleteRequests(#[from] LogDeleteRequestStoreError),
    #[error(transparent)]
    Rules(#[from] LokiRuleStoreError),
    #[error(transparent)]
    LimitsOverrides(#[from] OverridesError),
}

#[derive(Debug, Error)]
pub enum ServiceRuntimeError {
    #[error(transparent)]
    Config(#[from] ServiceConfigError),
    #[error(transparent)]
    Admin(#[from] AdminError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Producer(#[from] ProducerError),
    #[error(transparent)]
    Consumer(#[from] ConsumerError),
    #[error(transparent)]
    Compactor(#[from] CompactorRunError),
    #[error(transparent)]
    Frontier(#[from] CompactionFrontierStoreError),
    #[error(transparent)]
    DeleteRequests(#[from] LogDeleteRequestStoreError),
    #[error(transparent)]
    CriticalTask(#[from] CriticalTaskError),
    /// The TLS or authentication flags of the data port do not load.
    #[error(transparent)]
    ServerSecurity(#[from] ServerSecurityError),
    /// The TLS or SASL flags of the WAL connections do not load.
    #[error(transparent)]
    WalClientSecurity(#[from] WalClientSecurityError),
    /// The audit layer that the audit flags name does not start.
    #[error(transparent)]
    Audit(#[from] AuditBuildError),
}
