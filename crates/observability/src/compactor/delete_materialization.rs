#[allow(clippy::wildcard_imports)]
use super::*;

pub(crate) async fn poll_accumulated_log_compaction_records(
    consumer: &mut (impl LogWalConsumer + ?Sized),
    initial_timeout: Time,
    accumulation_window: Time,
    accumulation_poll_timeout: Time,
    max_records_per_batch: NonZeroUsize,
) -> Result<Vec<KafkaWalRecord>, WalConsumerError> {
    let mut records = consumer.poll(initial_timeout).await?;
    if records.is_empty() || records.len() >= max_records_per_batch.get() {
        return Ok(records);
    }

    let deadline = Instant::now() + accumulation_window.to_std();
    while records.len() < max_records_per_batch.get() {
        let remaining = deadline.saturating_duration_since(Instant::now()).as_time();
        if remaining <= <Time as TimeExt>::ZERO {
            break;
        }

        // `Time` is `PartialOrd` but not `Ord`, so `Time::min` rather than
        // `std::cmp::min`.
        let poll_timeout = remaining.min(accumulation_poll_timeout);
        let next = consumer.poll(poll_timeout).await?;
        if next.is_empty() {
            break;
        }
        records.extend(next);
    }

    Ok(records)
}

pub(crate) async fn materialize_delete_requests_in_existing_object_store_blocks(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    delete_requests: &SharedLogDeleteRequests,
) -> Result<(), CompactorRunError> {
    for tenant in active_log_delete_tenants(delete_requests)? {
        let mut materialized_blocks: BTreeMap<String, Option<BlockDescriptor>> = BTreeMap::new();

        match read_tenant_log_index_manifest_from_object_store(store, prefix, &tenant).await {
            Ok((label_index, block_index)) => {
                if let Some((next_label_index, next_block_index)) =
                    materialize_delete_requests_in_object_store_block_index(
                        store,
                        prefix,
                        &tenant,
                        &label_index,
                        &block_index,
                        delete_requests,
                        &mut materialized_blocks,
                    )
                    .await?
                {
                    write_tenant_log_index_manifest_to_object_store(
                        store,
                        prefix,
                        &tenant,
                        &next_label_index,
                        &next_block_index,
                    )
                    .await?;
                }
            }
            Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => {}
            Err(error) => return Err(error.into()),
        }

        let shard_ranges = match read_tenant_log_index_shard_ranges_from_object_store(
            store, prefix, &tenant,
        )
        .await
        {
            Ok(shard_ranges) => shard_ranges,
            Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => {
                continue;
            }
            Err(error) => return Err(error.into()),
        };

        for shard_range in shard_ranges {
            let (label_index, block_index) =
                read_tenant_log_index_shard_from_object_store(store, prefix, &tenant, shard_range)
                    .await?;
            if let Some((next_label_index, next_block_index)) =
                materialize_delete_requests_in_object_store_block_index(
                    store,
                    prefix,
                    &tenant,
                    &label_index,
                    &block_index,
                    delete_requests,
                    &mut materialized_blocks,
                )
                .await?
            {
                write_tenant_log_index_shard_to_object_store(
                    store,
                    prefix,
                    &tenant,
                    shard_range,
                    &next_label_index,
                    &next_block_index,
                )
                .await?;
            }
        }
    }
    Ok(())
}

#[cfg_attr(test, mutants::skip)]
pub(crate) fn materialize_delete_requests_in_existing_local_manifest_blocks(
    root: &FsPath,
    delete_requests: &SharedLogDeleteRequests,
) -> Result<(), CompactorRunError> {
    let (label_index, block_index) = match read_log_index_manifest(root) {
        Ok(indexes) => indexes,
        Err(BlockStoreError::Io(error)) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };

    let active_tenants = active_log_delete_tenants(delete_requests)?;
    if active_tenants.is_empty() {
        return Ok(());
    }

    let mut next_label_index = LabelIndex::default();
    let mut next_block_index = BlockIndex::default();
    let mut changed = false;

    for block in block_index.blocks() {
        let tenant = &block.key.tenant;
        let delete_filters = if active_tenants.contains(tenant) {
            active_log_delete_filters_from_requests(delete_requests, tenant, block.key.time_range)?
        } else {
            Vec::new()
        };
        let mut descriptor = block.clone();

        if !delete_filters.is_empty() {
            let rows = read_log_block(root, &block.key)?;
            let original_len = rows.len();
            let mut kept_rows = Vec::with_capacity(original_len);
            for row in rows {
                let labels = label_index
                    .labels_for(tenant, row.series_fingerprint)
                    .ok_or_else(|| CompactorRunError::MissingSeriesLabels {
                        tenant: tenant.clone(),
                        fingerprint: row.series_fingerprint,
                    })?;
                if is_deleted_log_entry(
                    &delete_filters,
                    labels,
                    &row.line,
                    &row.structured_metadata,
                    row.timestamp_ns,
                ) {
                    continue;
                }
                kept_rows.push(row);
            }

            if kept_rows.len() != original_len {
                changed = true;
                if kept_rows.is_empty() {
                    continue;
                }
                descriptor = write_log_block(root, &block.key, kept_rows)?;
            }
        }

        insert_descriptor_labels(&mut next_label_index, &label_index, tenant, &descriptor)?;
        next_block_index.insert(descriptor);
    }

    if changed {
        write_log_index_manifest(root, &next_label_index, &next_block_index)?;
    }
    Ok(())
}

#[cfg_attr(test, mutants::skip)]
pub(crate) async fn materialize_delete_requests_in_object_store_block_index(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
    delete_requests: &SharedLogDeleteRequests,
    materialized_blocks: &mut BTreeMap<String, Option<BlockDescriptor>>,
) -> Result<Option<(LabelIndex, BlockIndex)>, CompactorRunError> {
    let mut next_label_index = LabelIndex::default();
    let mut next_block_index = BlockIndex::default();
    let mut changed = false;

    for block in block_index.blocks() {
        let object_key = block.key.object_key();
        if let Some(materialized) = materialized_blocks.get(&object_key) {
            match materialized {
                Some(descriptor) => {
                    if descriptor != block {
                        changed = true;
                    }
                    insert_descriptor_labels(
                        &mut next_label_index,
                        label_index,
                        tenant,
                        descriptor,
                    )?;
                    next_block_index.insert(descriptor.clone());
                }
                None => {
                    changed = true;
                }
            }
            continue;
        }

        let delete_filters =
            active_log_delete_filters_from_requests(delete_requests, tenant, block.key.time_range)?;
        let mut descriptor = block.clone();

        if delete_filters.is_empty() {
            insert_descriptor_labels(&mut next_label_index, label_index, tenant, &descriptor)?;
            next_block_index.insert(descriptor);
            continue;
        }

        let rows = read_log_block_from_object_store(store, prefix, &block.key).await?;
        let original_len = rows.len();
        let mut kept_rows = Vec::with_capacity(original_len);
        for row in rows {
            let labels = label_index
                .labels_for(tenant, row.series_fingerprint)
                .ok_or_else(|| CompactorRunError::MissingSeriesLabels {
                    tenant: tenant.to_string(),
                    fingerprint: row.series_fingerprint,
                })?;
            if is_deleted_log_entry(
                &delete_filters,
                labels,
                &row.line,
                &row.structured_metadata,
                row.timestamp_ns,
            ) {
                continue;
            }
            kept_rows.push(row);
        }

        if kept_rows.len() != original_len {
            changed = true;
            if kept_rows.is_empty() {
                materialized_blocks.insert(object_key, None);
                continue;
            }
            descriptor =
                write_log_block_to_object_store(store, prefix, &block.key, kept_rows).await?;
            materialized_blocks.insert(object_key, Some(descriptor.clone()));
        }

        insert_descriptor_labels(&mut next_label_index, label_index, tenant, &descriptor)?;
        next_block_index.insert(descriptor);
    }

    Ok(changed.then_some((next_label_index, next_block_index)))
}

pub(crate) fn active_log_delete_tenants(
    delete_requests: &SharedLogDeleteRequests,
) -> Result<BTreeSet<String>, ActiveLogDeleteFilterError> {
    delete_requests.refresh()?;
    let requests = delete_requests
        .inner
        .lock()
        .expect("compactor delete state poisoned");
    Ok(requests
        .requests
        .iter()
        .map(|request| request.tenant.clone())
        .collect())
}

pub(crate) fn insert_descriptor_labels(
    target: &mut LabelIndex,
    source: &LabelIndex,
    tenant: &str,
    descriptor: &BlockDescriptor,
) -> Result<(), CompactorRunError> {
    for fingerprint in &descriptor.fingerprints {
        let labels = source.labels_for(tenant, *fingerprint).ok_or_else(|| {
            CompactorRunError::MissingSeriesLabels {
                tenant: tenant.to_string(),
                fingerprint: *fingerprint,
            }
        })?;
        target.insert_series(tenant.to_string(), labels.clone());
    }
    Ok(())
}

pub(crate) fn wal_compaction_chunks(records: Vec<WalLogRecord>) -> Vec<Vec<WalLogRecord>> {
    let mut chunks: Vec<Vec<WalLogRecord>> = Vec::new();
    for record in records {
        let Some(position) = record.position else {
            chunks.push(vec![record]);
            continue;
        };
        if let Some(chunk) = chunks.last_mut()
            && chunk.first().is_some_and(|first| {
                first.tenant == record.tenant
                    && first.position.is_some_and(|first_position| {
                        first_position.partition == position.partition
                    })
            })
        {
            chunk.push(record);
        } else {
            chunks.push(vec![record]);
        }
    }
    chunks
}

pub(crate) fn wal_record_time_range(
    records: &[WalLogRecord],
) -> Result<TimeRange, CompactionError> {
    let first = records.first().ok_or(CompactionError::EmptyWalBatch)?;
    let mut start_ns = first.timestamp_ns;
    let mut end_ns = first.timestamp_ns;
    for record in records.iter().skip(1) {
        start_ns = start_ns.min(record.timestamp_ns);
        end_ns = end_ns.max(record.timestamp_ns);
    }
    Ok(TimeRange::new(start_ns, end_ns)?)
}

pub(crate) type TenantCompactionIndexCache = BTreeMap<String, (LabelIndex, BlockIndex)>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LogCompactionIndexOutput {
    FullManifestAndShardCatalog,
    ShardManifests,
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn compact_log_block_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    key: &BlockKey,
    label_index: &LabelIndex,
    block_index: &mut BlockIndex,
    rows: Vec<LogRow>,
) -> Result<BlockDescriptor, BlockStoreError> {
    compact_log_block_to_object_store_with_index_output(
        store,
        prefix,
        key,
        label_index,
        block_index,
        rows,
        LogCompactionIndexOutput::FullManifestAndShardCatalog,
    )
    .await
}

pub(crate) async fn compact_log_block_to_object_store_with_index_output(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    key: &BlockKey,
    label_index: &LabelIndex,
    block_index: &mut BlockIndex,
    rows: Vec<LogRow>,
    index_output: LogCompactionIndexOutput,
) -> Result<BlockDescriptor, BlockStoreError> {
    let descriptor = write_log_block_to_object_store(store, prefix, key, rows).await?;
    block_index.insert(descriptor.clone());
    write_tenant_compaction_indexes_to_object_store(
        store,
        prefix,
        &key.tenant,
        &descriptor,
        label_index,
        block_index,
        index_output,
    )
    .await?;
    Ok(descriptor)
}

pub(crate) async fn write_tenant_compaction_indexes_to_object_store(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    tenant: &str,
    new_descriptor: &BlockDescriptor,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
    index_output: LogCompactionIndexOutput,
) -> Result<(), BlockStoreError> {
    if index_output == LogCompactionIndexOutput::ShardManifests {
        let mut shard_block_index = BlockIndex::default();
        shard_block_index.insert(new_descriptor.clone());
        write_tenant_log_index_shard_to_object_store(
            store,
            prefix,
            tenant,
            new_descriptor.key.time_range,
            label_index,
            &shard_block_index,
        )
        .await?;
        return Ok(());
    }

    if index_output == LogCompactionIndexOutput::FullManifestAndShardCatalog {
        write_tenant_log_index_manifest_to_object_store(
            store,
            prefix,
            tenant,
            label_index,
            block_index,
        )
        .await?;
    }

    write_tenant_log_index_shard_to_object_store(
        store,
        prefix,
        tenant,
        new_descriptor.key.time_range,
        label_index,
        block_index,
    )
    .await?;

    let mut shard_ranges =
        match read_tenant_log_index_shard_ranges_from_object_store(store, prefix, tenant).await {
            Ok(shard_ranges) => shard_ranges,
            Err(BlockStoreError::ObjectStore(object_store::Error::NotFound { .. })) => Vec::new(),
            Err(error) => return Err(error),
        };
    if !shard_ranges.contains(&new_descriptor.key.time_range) {
        shard_ranges.push(new_descriptor.key.time_range);
    }
    shard_ranges.sort_by_key(|range| (range.start_ns, range.end_ns));
    write_tenant_log_index_shard_catalog_to_object_store(store, prefix, tenant, &shard_ranges).await
}

pub trait CompactionOffsetCommitter {
    /// # Errors
    /// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
    fn commit_compacted(&mut self, position: WalPosition) -> Result<(), CompactionCommitError>;
}

#[derive(Debug, Error)]
#[error("offset commit failed")]
pub struct CompactionCommitError;

#[derive(Debug, Error)]
pub enum CompactionError {
    #[error("cannot compact an empty WAL batch")]
    EmptyWalBatch,
    #[error("cannot compact WAL batch after all rows were deleted")]
    AllRowsDeleted,
    #[error("missing WAL position for record at timestamp {timestamp_ns}")]
    MissingWalPosition { timestamp_ns: i64 },
    #[error("cannot compact mixed-tenant WAL batch: expected {expected}, got {actual}")]
    MixedTenant { expected: String, actual: String },
    #[error("cannot compact mixed-partition WAL batch: expected {expected}, got {actual}")]
    MixedPartition { expected: i32, actual: i32 },
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error(transparent)]
    Commit(#[from] CompactionCommitError),
}
