use super::{
    ActiveLogDeleteFilterError, BlockStoreError, CompactionError, CompactionFrontierStoreError,
    Error, KafkaWalCompactionError, SeriesFingerprint, WalConsumerError, WalRecordDecodeError,
};

#[derive(Debug, Error)]
pub enum CompactorRunError {
    #[error(transparent)]
    Wal(#[from] KafkaWalCompactionError),
    #[error(transparent)]
    Decode(#[from] WalRecordDecodeError),
    #[error(transparent)]
    Compaction(#[from] CompactionError),
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error(transparent)]
    Consumer(#[from] WalConsumerError),
    #[error(transparent)]
    Frontier(#[from] CompactionFrontierStoreError),
    #[error(transparent)]
    DeleteFilter(#[from] ActiveLogDeleteFilterError),
    #[error("missing labels for tenant `{tenant}` series fingerprint {fingerprint}")]
    MissingSeriesLabels {
        tenant: String,
        fingerprint: SeriesFingerprint,
    },
    #[error("compacted WAL batch did not report a commit position")]
    MissingCommitPosition,
}
