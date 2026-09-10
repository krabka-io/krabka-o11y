use super::{
    Arc, BlockMeta, BlockStoreError, BlockStreamWriter, BlockWriter, ByteSize, CompactedBlockStats,
    MERGE_BATCH_ROWS, MERGE_READ_BATCH_ROWS, ObjectStore, RecordBatch, SCOL_START_NANO,
    SCOL_TRACE_ID, SortedMerge, StreamExt, SummaryColumns, TraceGroupBuffer, TraceIndex,
    TracesError, align_batch_to_block_schema, merged_promoted_attrs, open_block_stream,
    recompute_nested_sets, recompute_trace_level_columns, span_block_decl,
    span_block_schema_with_promoted_attrs,
};

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
    let mut runs = Vec::with_capacity(input_keys.len());
    for key in input_keys {
        let (schema, batches) =
            open_block_stream(store.clone(), key, block_read_max, MERGE_READ_BATCH_ROWS)
                .await
                .map_err(|err| TracesError::Block(err.to_string()))?;
        schemas.push(schema);
        runs.push(batches);
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
    let mut merge = SortedMerge::new(schema.clone(), &decl.sort_key, runs, MERGE_BATCH_ROWS)
        .map_err(|err| TracesError::Block(err.to_string()))?;
    let mut block = writer
        .open_block(
            tenant,
            output_key,
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
