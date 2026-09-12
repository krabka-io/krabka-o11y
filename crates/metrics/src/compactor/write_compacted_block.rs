use krabka_blockstore::{BlockSchema, BlockWriter, SummaryColumns};

use super::{
    CompactedBlockRequest, CompactedBlockWrite, CompactionIndexManifest, CompactionIndexSink,
    CompactionWriteError, MetricBlockKind, compaction_object_plan,
    compaction_partition_object_plan,
};
use crate::clock_reading_decl;

fn block_decl(kind: MetricBlockKind) -> Option<BlockSchema> {
    (kind == MetricBlockKind::ClockReadings).then(clock_reading_decl)
}

pub(crate) async fn write_compacted_block<S>(
    block_writer: &BlockWriter,
    index_sink: &S,
    request: CompactedBlockRequest<'_>,
) -> Result<CompactedBlockWrite, CompactionWriteError>
where
    S: CompactionIndexSink + ?Sized,
{
    let mut plan = request.partition.map_or_else(
        || {
            compaction_object_plan(
                request.tenant,
                request.kind,
                request.first_offset,
                request.last_offset,
            )
        },
        |partition| {
            compaction_partition_object_plan(
                request.tenant,
                request.kind,
                partition,
                request.first_offset,
                request.last_offset,
            )
        },
    );
    plan.row_count = request.batch.num_rows();
    let schema = request.batch.schema();
    let block_meta = if let Some(decl) = block_decl(request.kind) {
        block_writer
            .write_block_with_decl(
                request.tenant,
                &plan.block_key,
                schema,
                &[request.batch],
                &decl,
                SummaryColumns::series(),
            )
            .await?
    } else {
        block_writer
            .write_block(request.tenant, &plan.block_key, schema, &[request.batch])
            .await?
    };
    let manifest =
        CompactionIndexManifest::from_block_meta(request.kind, &plan, &block_meta, request.series);
    index_sink.write_manifest(&manifest).await?;

    Ok(CompactedBlockWrite {
        kind: request.kind,
        block_meta,
        manifest,
    })
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::{MetricBlockKind, block_decl};

    #[test]
    fn only_clock_blocks_have_a_signal_specific_declaration() {
        check!(block_decl(MetricBlockKind::ClockReadings).is_some());
        for kind in [
            MetricBlockKind::Float,
            MetricBlockKind::NativeHistograms,
            MetricBlockKind::Exemplars,
            MetricBlockKind::Metadata,
        ] {
            check!(block_decl(kind).is_none(), "kind: {kind:?}");
        }
    }
}
