use super::*;

pub(crate) async fn advance_and_persist_compaction_frontier(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    frontier: &SharedCompactionFrontier,
    descriptor: &BlockDescriptor,
) -> Result<(), CompactorRunError> {
    frontier.advance_partition_offset(WalPosition {
        partition: PartitionIndex(descriptor.key.partition),
        offset: Offset(descriptor.key.last_offset),
    });
    write_compaction_frontier_to_object_store(store, prefix, &frontier.snapshot()).await?;
    Ok(())
}
