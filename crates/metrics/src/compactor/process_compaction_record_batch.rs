use super::{
    BlockWriter, CompactionBatchResult, CompactionIndexSink, CompactionOffsetCommitter,
    CompactionWalRecord, CompactionWindowError, write_compaction_batch_windows,
};

/// Processes a polled compaction batch by partition, and keeps the per-partition
/// commits.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn process_compaction_record_batch<S, C>(
    block_writer: &BlockWriter,
    index_sink: &S,
    committer: &C,
    records: &[CompactionWalRecord],
) -> Result<CompactionBatchResult, CompactionWindowError>
where
    S: CompactionIndexSink + ?Sized,
    C: CompactionOffsetCommitter + ?Sized,
{
    // Write every partition's block + index sidecar durably BEFORE committing any
    // offsets. The production committer (`CompactionConsumerCommitter`) commits
    // the whole assignment's offsets regardless of the per-partition offset
    // passed, so committing per-partition would advance partitions whose blocks
    // are not yet written; a later partition's write failure would then skip
    // those un-written records — silent data loss. One commit after all writes
    // only advances past fully-durable data; any write error returns before the
    // commit so the next poll re-reads from the last committed offset
    // (at-least-once).
    let batch = write_compaction_batch_windows(block_writer, index_sink, records).await?;

    if !batch.committed_offsets.is_empty() {
        committer.commit_offsets(&batch.committed_offsets).await?;
    }

    Ok(batch)
}
