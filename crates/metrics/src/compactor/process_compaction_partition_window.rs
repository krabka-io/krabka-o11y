use super::{
    BlockWriter, CompactionIndexSink, CompactionOffsetCommitter, CompactionWalRecord,
    CompactionWindowError, CompactionWindowResult, write_compaction_partition_window,
};

/// Decodes, compacts, writes, and commits one assigned WAL partition window.
///
/// A successful return means that durable block and index writes represent all
/// decoded records in the window, and that the partition offset has moved to the
/// next offset. An empty window does nothing.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub async fn process_compaction_partition_window<S, C>(
    block_writer: &BlockWriter,
    index_sink: &S,
    committer: &C,
    records: &[CompactionWalRecord],
) -> Result<CompactionWindowResult, CompactionWindowError>
where
    S: CompactionIndexSink + ?Sized,
    C: CompactionOffsetCommitter + ?Sized,
{
    let result = write_compaction_partition_window(block_writer, index_sink, records).await?;
    if let Some(committed_offset) = result.committed_offset.clone() {
        committer
            .commit_offsets(std::slice::from_ref(&committed_offset))
            .await?;
    }
    Ok(result)
}
