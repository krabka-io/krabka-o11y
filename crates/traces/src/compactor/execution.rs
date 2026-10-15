use super::*;

/// What one compaction pass produced, and what it retired to produce it.
///
/// The two travel together because the caller has to delete the inputs, and it
/// can only do that after the index that no longer names them is durable. A
/// pass that returned its outputs alone would leave the caller no way to name
/// the objects it must now delete, and every pass would double the storage of
/// the range it merged.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CompactionPassOutcome {
    /// The replacement blocks, one per planned job.
    pub outputs: Vec<BlockMeta>,
    /// The input blocks the index no longer names. They are still in object
    /// storage, and the caller deletes them once the index is saved.
    pub retired_inputs: Vec<String>,
}

impl CompactionPassOutcome {
    /// Whether the pass planned nothing at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.outputs.is_empty() && self.retired_inputs.is_empty()
    }
}

/// Merge existing span blocks with a caller-supplied on-disk read limit.
///
/// The inputs are each already in the declared `[trace_id, start_unix_nano]`
/// order, so they are merged rather than concatenated and sorted: the merge
/// holds one batch of each input, the writer takes the merged batches in the
/// order it wants them, and the memory the whole pass needs is a function of
/// the output batch size and the largest single trace rather than of the total
/// bytes read.
///
/// # Errors
/// Returns an error when an input exceeds the configured cap, an input block
/// is malformed, or the backing span store fails.
pub async fn compact_block_keys_with_max_bytes(
    store: Arc<dyn ObjectStore>,
    writer: &BlockWriter,
    index: &mut TraceIndex,
    tenant: &str,
    input_keys: &[String],
    output_key: &str,
    block_read_max: ByteSize,
) -> Result<BlockMeta, TracesError> {
    if input_keys.is_empty() {
        return Err(TracesError::Block("cannot compact empty block set".into()));
    }

    // Open every input before reading any of it. The inputs' own schemas
    // decide the output's: a block written while `--promote-span-attr` was set
    // is wider than the base schema, and an input set can straddle a change to
    // those flags, so the output carries the union of every input's promoted
    // columns and each input is widened to match. Rebuilding the base schema
    // here instead would fail outright on the first promoted input.
    let mut schemas = Vec::with_capacity(input_keys.len());
    let mut input_versions = Vec::with_capacity(input_keys.len());
    let mut runs = Vec::with_capacity(input_keys.len());
    for key in input_keys {
        let stream = open_block_stream(store.clone(), key, block_read_max, MERGE_READ_BATCH_ROWS)
            .await
            .map_err(|err| TracesError::Block(err.to_string()))?;
        input_versions.push(stream.meta);
        schemas.push(stream.schema);
        runs.push(stream.batches);
    }

    // `merged_promoted_attrs` reads its inputs' schemas and nothing else, so
    // a row-less batch per input asks it the same question the blocks would
    // without any of them being read.
    let empty = schemas
        .into_iter()
        .map(RecordBatch::new_empty)
        .collect::<Vec<_>>();
    let promoted_attrs = merged_promoted_attrs(&empty)?;
    let schema = span_block_schema_with_promoted_attrs(&promoted_attrs);

    let runs = runs
        .into_iter()
        .map(|batches| {
            let schema = schema.clone();
            batches
                .map(move |batch| {
                    align_batch_to_block_schema(&batch?, &schema)
                        .map_err(|err| BlockStoreError::InvalidBlock(err.to_string()))
                })
                .boxed()
        })
        .collect::<Vec<_>>();

    let decl = span_block_decl();
    let output_key = versioned_compaction_key(output_key, &input_versions);
    let mut merge = SortedMerge::new(schema.clone(), &decl.sort_key, runs, MERGE_BATCH_ROWS)
        .map_err(|err| TracesError::Block(err.to_string()))?;
    let mut block = writer
        .open_block(
            tenant,
            &output_key,
            schema.clone(),
            &decl,
            SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
        )
        .map_err(|err| TracesError::Block(err.to_string()))?;

    let mut buffer = TraceGroupBuffer::new(schema);
    let mut stats = CompactedBlockStats::new();
    let drained: Result<(), TracesError> = async {
        while let Some(merged) = merge
            .next_batch()
            .await
            .map_err(|err| TracesError::Block(err.to_string()))?
        {
            buffer.push(merged);
            while let Some(complete) = buffer.take_complete(MERGE_BATCH_ROWS)? {
                write_complete_traces(&mut block, &mut stats, &complete).await?;
            }
        }
        if let Some(rest) = buffer.take_rest()? {
            write_complete_traces(&mut block, &mut stats, &rest).await?;
        }
        Ok(())
    }
    .await;
    if let Err(error) = drained {
        block
            .abort()
            .await
            .map_err(|err| TracesError::Block(err.to_string()))?;
        return Err(error);
    }

    let mut meta = block
        .finish()
        .await
        .map_err(|err| TracesError::Block(err.to_string()))?;
    // The index derives the level from the blocks being retired and hands it
    // back, so the meta this returns says what the index recorded rather than
    // the level-zero the writer stamped.
    meta.level = index.replace_trace_blocks(tenant, input_keys, stats.into_block_stats(&meta));
    Ok(meta)
}

/// Renumbers and re-denormalizes a run of complete traces, then writes it.
///
/// Both recomputations group by `trace_id` and touch no column the sort key
/// names, so running them over a run of complete traces gives what running
/// them over the whole merged block would, and the rows stay in the order the
/// writer was promised.
async fn write_complete_traces(
    block: &mut BlockStreamWriter,
    stats: &mut CompactedBlockStats,
    batch: &RecordBatch,
) -> Result<(), TracesError> {
    let batch = recompute_nested_sets(batch)?;
    let batch = recompute_trace_level_columns(&batch)?;
    stats.push(&batch)?;
    block
        .write_batch(&batch)
        .await
        .map_err(|err| TracesError::Block(err.to_string()))
}

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
