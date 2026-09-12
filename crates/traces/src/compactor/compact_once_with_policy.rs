use super::{
    Arc, BlockWriter, ByteSize, CompactionPassOutcome, CompactionPolicy, ObjectStore, TraceIndex,
    TracesError, compact_block_keys_with_max_bytes, plan_compactions, planned_compacted_object_key,
    prefixed_object_key,
};

/// Runs one compaction pass over the whole index.
///
/// A pass plans and executes; it does not loop. Running it again picks up
/// where this one left off, one rung further up the ladder, and eventually
/// plans nothing. Which blocks meet, and when the climbing stops, is the
/// policy's business rather than a time window an operator types in.
///
/// The pass replaces the inputs in the index and leaves their objects in
/// place. It names them in [`CompactionPassOutcome::retired_inputs`], and the
/// caller deletes them once the index that no longer names them is durable.
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
) -> Result<CompactionPassOutcome, TracesError> {
    let mut outcome = CompactionPassOutcome::default();
    for job in plan_compactions(index, policy) {
        let output_key =
            prefixed_object_key(object_key_prefix, &planned_compacted_object_key(&job));
        let meta = compact_block_keys_with_max_bytes(
            store.clone(),
            writer,
            index,
            &job.tenant,
            &job.input_keys,
            &output_key,
            block_read_max,
        )
        .await?;
        // A job whose output reuses an input's key retires nothing: the object
        // the index now names is the one that key holds.
        outcome.retired_inputs.extend(
            job.input_keys
                .into_iter()
                .filter(|key| *key != meta.object_key),
        );
        outcome.outputs.push(meta);
    }
    Ok(outcome)
}
