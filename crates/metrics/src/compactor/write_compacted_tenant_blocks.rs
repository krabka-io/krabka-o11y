use super::{
    BlockWriter, CompactedBlockWrite, CompactionIndexSink, CompactionWriteError,
    TenantCompactionRows, write_compacted_tenant_blocks_with_partition,
};

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
