use super::{CompactionIndexSink, BlockWriter, TenantCompactionRows, PartitionIndex, CompactedBlockWrite, CompactionWriteError, encode_tenant_batches, write_compacted_block, CompactedBlockRequest, MetricBlockKind, series_labels_for_kind};

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
