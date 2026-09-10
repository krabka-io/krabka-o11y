use super::{
    BTreeMap, BlockWriter, CompactionBatchResult, CompactionCommitError,
    CompactionConsumerCommitMut, CompactionIndexSink, CompactionWalRecord, CompactionWindowError,
    PartitionIndex, write_compaction_partition_window,
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
    // Write every partition's block durably BEFORE committing any offsets.
    // Commit only the offsets produced by the durable writes below. A consumer
    // position can include records that are not in this flush, so committing a
    // whole assignment would be a data-loss boundary.
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
        consumer
            .commit_offsets_sync_mut(&committed_offsets)
            .await
            .map_err(|error| CompactionCommitError::Commit(error.to_string()))?;
    }

    Ok(CompactionBatchResult {
        partition_results,
        writes,
        committed_offsets,
    })
}
