use super::{
    BlockWriter, CompactedBlockRequest, CompactedBlockWrite, CompactionIndexManifest,
    CompactionIndexSink, CompactionWriteError, compaction_object_plan,
    compaction_partition_object_plan,
};

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
    let block_meta = block_writer
        .write_block(
            request.tenant,
            &plan.block_key,
            request.batch.schema(),
            &[request.batch],
        )
        .await?;
    let manifest =
        CompactionIndexManifest::from_block_meta(request.kind, &plan, &block_meta, request.series);
    index_sink.write_manifest(&manifest).await?;

    Ok(CompactedBlockWrite {
        kind: request.kind,
        block_meta,
        manifest,
    })
}
