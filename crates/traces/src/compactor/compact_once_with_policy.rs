use super::{
    Arc, BlockMeta, BlockWriter, ByteSize, CompactionPolicy, ObjectStore, TraceIndex, TracesError,
    compact_block_keys_with_max_bytes, plan_compactions, planned_compacted_object_key,
    prefixed_object_key,
};

/// Runs one compaction pass over the whole index.
///
/// A pass plans and executes; it does not loop. Running it again picks up
/// where this one left off, one rung further up the ladder, and eventually
/// plans nothing. Which blocks meet, and when the climbing stops, is the
/// policy's business rather than a time window an operator types in.
///
/// # Errors
/// Returns an error when an input exceeds the configured cap, an input block
/// is malformed, or the backing span store fails.
pub async fn compact_once_with_policy(
    store: Arc<dyn ObjectStore>,
    writer: &BlockWriter,
    index: &mut TraceIndex,
    object_key_prefix: &str,
    policy: CompactionPolicy,
    block_read_max: ByteSize,
) -> Result<Vec<BlockMeta>, TracesError> {
    let mut metas = Vec::new();
    for job in plan_compactions(index, policy) {
        let output_key =
            prefixed_object_key(object_key_prefix, &planned_compacted_object_key(&job));
        metas.push(
            compact_block_keys_with_max_bytes(
                store.clone(),
                writer,
                index,
                &job.tenant,
                &job.input_keys,
                &output_key,
                block_read_max,
            )
            .await?,
        );
    }
    Ok(metas)
}
