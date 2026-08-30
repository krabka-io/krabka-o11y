use super::{BTreeMap, BlockWriter, CompactionBatchResult, CompactionIndexSink, CompactionOffsetCommitter, CompactionWalRecord, CompactionWindowError, PartitionIndex, write_compaction_partition_window};

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
    let mut by_partition = BTreeMap::<PartitionIndex, Vec<CompactionWalRecord>>::new();
    for record in records {
        by_partition
            .entry(record.partition)
            .or_default()
            .push(record.clone());
    }

    let mut partition_results = Vec::new();
    let mut writes = Vec::new();
    let mut committed_offsets = Vec::new();
    // Write every partition's block + index sidecar durably BEFORE committing any
    // offsets. The production committer (`CompactionConsumerCommitter`) commits
    // the whole assignment's offsets regardless of the per-partition offset
    // passed, so committing per-partition would advance partitions whose blocks
    // are not yet written; a later partition's write failure would then skip
    // those un-written records — silent data loss. One commit after all writes
    // only advances past fully-durable data; any write error returns before the
    // commit so the next poll re-reads from the last committed offset
    // (at-least-once).
    for partition_records in by_partition.into_values() {
        let result =
            write_compaction_partition_window(block_writer, index_sink, &partition_records).await?;
        writes.extend(result.writes.clone());
        if let Some(offset) = &result.committed_offset {
            committed_offsets.push(offset.clone());
        }
        partition_results.push(result);
    }

    if !committed_offsets.is_empty() {
        committer.commit_offsets(&committed_offsets).await?;
    }

    Ok(CompactionBatchResult {
        partition_results,
        writes,
        committed_offsets,
    })
}
