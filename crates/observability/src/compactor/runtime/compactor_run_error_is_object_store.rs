use super::*;

pub(crate) fn compactor_run_error_is_object_store(error: &CompactorRunError) -> bool {
    match error {
        CompactorRunError::Wal(KafkaWalCompactionError::Compaction(error))
        | CompactorRunError::Compaction(error) => compaction_error_is_object_store(error),
        CompactorRunError::BlockStore(error) => block_store_error_is_object_store(error),
        CompactorRunError::Frontier(CompactionFrontierStoreError::ObjectStore(_)) => true,
        CompactorRunError::Wal(KafkaWalCompactionError::Decode(_))
        | CompactorRunError::Decode(_)
        | CompactorRunError::Consumer(_)
        | CompactorRunError::Frontier(
            CompactionFrontierStoreError::InvalidVersion { .. }
            | CompactionFrontierStoreError::Json(_),
        )
        | CompactorRunError::DeleteFilter(_)
        | CompactorRunError::MissingSeriesLabels { .. }
        | CompactorRunError::MissingCommitPosition => false,
    }
}
