use super::*;

pub(crate) async fn write_compaction_partition_window<S>(
    block_writer: &BlockWriter,
    index_sink: &S,
    records: &[CompactionWalRecord],
) -> Result<CompactionWindowResult, CompactionWindowError>
where
    S: CompactionIndexSink + ?Sized,
{
    let Some(first_record) = records.first() else {
        return Ok(CompactionWindowResult {
            writes: Vec::new(),
            committed_offset: None,
        });
    };
    let partition = first_record.partition;
    let mut first_offset = first_record.offset;
    let mut last_offset = first_record.offset;
    let mut wal_records = Vec::with_capacity(records.len());

    for record in records {
        if record.partition != partition {
            return Err(CompactionWindowError::MultiplePartitions {
                first: partition,
                second: record.partition,
            });
        }
        first_offset = first_offset.min(record.offset);
        last_offset = last_offset.max(record.offset);
        wal_records.push(WalRecord::decode(&record.value)?);
    }

    let mut writes = Vec::new();
    for rows in compact_wal_records(&wal_records) {
        writes.extend(
            write_compacted_tenant_partition_blocks(
                block_writer,
                index_sink,
                &rows,
                partition,
                first_offset.0,
                last_offset.0,
            )
            .await?,
        );
    }

    let committed_offset = CompactionPartitionOffset {
        partition,
        offset: last_offset + 1,
    };

    Ok(CompactionWindowResult {
        writes,
        committed_offset: Some(committed_offset),
    })
}

/// Writes a polled compaction batch durably, one window per partition, and
/// answers with the offsets those writes cover. Nothing is committed: the
/// caller commits `committed_offsets` only once every partition is written,
/// so an error here returns before any commit and the next poll re-reads from
/// the last committed offset (at-least-once).
pub(crate) async fn write_compaction_batch_windows<S>(
    block_writer: &BlockWriter,
    index_sink: &S,
    records: &[CompactionWalRecord],
) -> Result<CompactionBatchResult, CompactionWindowError>
where
    S: CompactionIndexSink + ?Sized,
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
    for partition_records in by_partition.into_values() {
        let result =
            write_compaction_partition_window(block_writer, index_sink, &partition_records).await?;
        writes.extend(result.writes.clone());
        if let Some(offset) = &result.committed_offset {
            committed_offsets.push(offset.clone());
        }
        partition_results.push(result);
    }

    Ok(CompactionBatchResult {
        partition_results,
        writes,
        committed_offsets,
    })
}
