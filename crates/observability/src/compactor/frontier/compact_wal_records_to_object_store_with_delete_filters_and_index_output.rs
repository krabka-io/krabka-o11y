use super::*;

pub(crate) async fn compact_wal_records_to_object_store_with_delete_filters_and_index_output(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    label_index: &mut LabelIndex,
    block_index: &mut BlockIndex,
    committer: &mut impl CompactionOffsetCommitter,
    records: Vec<WalLogRecord>,
    output: (&[ActiveLogDeleteFilter], LogCompactionIndexOutput),
) -> Result<Option<BlockDescriptor>, CompactionError> {
    let (delete_filters, index_output) = output;
    let first = records.first().ok_or(CompactionError::EmptyWalBatch)?;
    let tenant = first.tenant.clone();
    let first_position = first.position.ok_or(CompactionError::MissingWalPosition {
        timestamp_ns: first.timestamp_ns,
    })?;
    let partition = first_position.partition;
    let mut first_offset = first_position.offset;
    let mut last_offset = first_position.offset;
    let mut start_ns = first.timestamp_ns;
    let mut end_ns = first.timestamp_ns;
    let mut staged_label_index = label_index.clone();
    let mut rows = Vec::with_capacity(records.len());

    for record in records {
        if record.tenant != tenant {
            return Err(CompactionError::MixedTenant {
                expected: tenant,
                actual: record.tenant,
            });
        }
        let position = record.position.ok_or(CompactionError::MissingWalPosition {
            timestamp_ns: record.timestamp_ns,
        })?;
        if position.partition != partition {
            return Err(CompactionError::MixedPartition {
                expected: partition.get(),
                actual: position.partition.get(),
            });
        }

        first_offset = first_offset.min(position.offset);
        last_offset = last_offset.max(position.offset);
        start_ns = start_ns.min(record.timestamp_ns);
        end_ns = end_ns.max(record.timestamp_ns);
        if is_deleted_log_entry(
            delete_filters,
            &record.labels,
            &record.line,
            &record.structured_metadata,
            record.timestamp_ns,
        ) {
            continue;
        }
        let fingerprint = staged_label_index.insert_series(&tenant, record.labels);
        rows.push(LogRow::new(
            fingerprint,
            record.timestamp_ns,
            record.line,
            record.structured_metadata,
        ));
    }

    if rows.is_empty() {
        committer.commit_compacted(WalPosition {
            partition,
            offset: last_offset,
        })?;
        return Ok(None);
    }

    let key = BlockKey::new(
        tenant,
        partition.get(),
        first_offset.get(),
        last_offset.get(),
        TimeRange::new(start_ns, end_ns)?,
    );
    let mut staged_block_index = block_index.clone();
    let descriptor = compact_log_block_to_object_store_with_index_output(
        store,
        prefix,
        &key,
        &staged_label_index,
        &mut staged_block_index,
        rows,
        index_output,
    )
    .await?;

    committer.commit_compacted(WalPosition {
        partition,
        offset: last_offset,
    })?;
    *label_index = staged_label_index;
    *block_index = staged_block_index;

    Ok(Some(descriptor))
}
