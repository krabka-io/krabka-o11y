use super::*;

pub(crate) fn compaction_error_is_object_store(error: &CompactionError) -> bool {
    match error {
        CompactionError::BlockStore(error) => block_store_error_is_object_store(error),
        CompactionError::EmptyWalBatch
        | CompactionError::AllRowsDeleted
        | CompactionError::MissingWalPosition { .. }
        | CompactionError::MixedTenant { .. }
        | CompactionError::MixedPartition { .. }
        | CompactionError::Commit(_) => false,
    }
}
