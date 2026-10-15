use super::*;

/// Writes all non-empty block kinds for a compacted tenant window.
///
/// This function writes the block object before the matching index sidecar, so
/// a caller can safely commit WAL consumer offsets only after this function
/// returns successfully.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn write_compacted_tenant_blocks<S>(
    block_writer: &BlockWriter,
    index_sink: &S,
    rows: &TenantCompactionRows,
    first_offset: i64,
    last_offset: i64,
) -> Result<Vec<CompactedBlockWrite>, CompactionWriteError>
where
    S: CompactionIndexSink + ?Sized,
{
    write_compacted_tenant_blocks_with_partition(
        block_writer,
        index_sink,
        rows,
        None,
        first_offset,
        last_offset,
    )
    .await
}

pub(crate) async fn write_compacted_tenant_blocks_with_partition<S>(
    block_writer: &BlockWriter,
    index_sink: &S,
    rows: &TenantCompactionRows,
    partition: Option<PartitionIndex>,
    first_offset: i64,
    last_offset: i64,
) -> Result<Vec<CompactedBlockWrite>, CompactionWriteError>
where
    S: CompactionIndexSink + ?Sized,
{
    let batches = encode_tenant_batches(rows)?;
    let mut writes = Vec::new();

    if let Some(batch) = batches.float {
        writes.push(
            write_compacted_block(
                block_writer,
                index_sink,
                CompactedBlockRequest {
                    tenant: &rows.tenant,
                    kind: MetricBlockKind::Float,
                    partition,
                    first_offset,
                    last_offset,
                    batch,
                    series: series_labels_for_kind(rows, MetricBlockKind::Float),
                },
            )
            .await?,
        );
    }
    if let Some(batch) = batches.native_histograms {
        writes.push(
            write_compacted_block(
                block_writer,
                index_sink,
                CompactedBlockRequest {
                    tenant: &rows.tenant,
                    kind: MetricBlockKind::NativeHistograms,
                    partition,
                    first_offset,
                    last_offset,
                    batch,
                    series: series_labels_for_kind(rows, MetricBlockKind::NativeHistograms),
                },
            )
            .await?,
        );
    }
    if let Some(batch) = batches.exemplars {
        writes.push(
            write_compacted_block(
                block_writer,
                index_sink,
                CompactedBlockRequest {
                    tenant: &rows.tenant,
                    kind: MetricBlockKind::Exemplars,
                    partition,
                    first_offset,
                    last_offset,
                    batch,
                    series: series_labels_for_kind(rows, MetricBlockKind::Exemplars),
                },
            )
            .await?,
        );
    }
    if let Some(batch) = batches.metadata {
        writes.push(
            write_compacted_block(
                block_writer,
                index_sink,
                CompactedBlockRequest {
                    tenant: &rows.tenant,
                    kind: MetricBlockKind::Metadata,
                    partition,
                    first_offset,
                    last_offset,
                    batch,
                    series: series_labels_for_kind(rows, MetricBlockKind::Metadata),
                },
            )
            .await?,
        );
    }
    if let Some(batch) = batches.clock_readings {
        writes.push(
            write_compacted_block(
                block_writer,
                index_sink,
                CompactedBlockRequest {
                    tenant: &rows.tenant,
                    kind: MetricBlockKind::ClockReadings,
                    partition,
                    first_offset,
                    last_offset,
                    batch,
                    series: series_labels_for_kind(rows, MetricBlockKind::ClockReadings),
                },
            )
            .await?,
        );
    }

    Ok(writes)
}

/// Writes all non-empty block kinds for a compacted tenant partition window.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn write_compacted_tenant_partition_blocks<S>(
    block_writer: &BlockWriter,
    index_sink: &S,
    rows: &TenantCompactionRows,
    partition: PartitionIndex,
    first_offset: i64,
    last_offset: i64,
) -> Result<Vec<CompactedBlockWrite>, CompactionWriteError>
where
    S: CompactionIndexSink + ?Sized,
{
    write_compacted_tenant_blocks_with_partition(
        block_writer,
        index_sink,
        rows,
        Some(partition),
        first_offset,
        last_offset,
    )
    .await
}
