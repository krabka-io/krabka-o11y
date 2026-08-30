use super::{
    BlockStoreError, CompactionFrontierStoreError, Error, LogDeleteRequestStoreError,
    LokiRuleStoreError, QuerierIndexSource,
};

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
    DeleteRequests(#[from] LogDeleteRequestStoreError),
    #[error(transparent)]
    Rules(#[from] LokiRuleStoreError),
}
