use super::{
    BlockWriter, CompactionIndexSink, CompactionPartitionOffset, CompactionWalRecord,
    CompactionWindowError, CompactionWindowResult, WalRecord, compact_wal_records,
    write_compacted_tenant_partition_blocks,
};

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
