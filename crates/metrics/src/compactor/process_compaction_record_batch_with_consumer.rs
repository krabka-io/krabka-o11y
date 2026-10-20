use super::{
    BlockWriter, CompactionBatchResult, CompactionCommitError, CompactionConsumerCommitMut,
    CompactionIndexSink, CompactionWalRecord, CompactionWindowError,
    write_compaction_batch_windows,
};

pub(crate) async fn process_compaction_record_batch_with_consumer<S, C>(
    block_writer: &BlockWriter,
    index_sink: &S,
    consumer: &mut C,
    records: &[CompactionWalRecord],
) -> Result<CompactionBatchResult, CompactionWindowError>
where
    S: CompactionIndexSink + ?Sized,
    C: CompactionConsumerCommitMut + ?Sized,
{
    // Write every partition's block durably BEFORE committing any offsets.
    // Commit only the offsets produced by the durable writes below. A consumer
    // position can include records that are not in this flush, so committing a
    // whole assignment would be a data-loss boundary.
    let batch = write_compaction_batch_windows(block_writer, index_sink, records).await?;

    if !batch.committed_offsets.is_empty() {
        consumer
            .commit_offsets_sync_mut(&batch.committed_offsets)
            .await
            .map_err(|error| CompactionCommitError::Commit(error.to_string()))?;
    }

    Ok(batch)
}
