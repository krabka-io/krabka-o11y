use super::{Arc, ObjectStore, BlockWriter, TraceIndex, ByteSize, BlockMeta, TracesError, read_block_with_max_bytes, Array, span_block_schema, concat_batches, recompute_nested_sets, recompute_trace_level_columns, span_block_decl, SummaryColumns, SCOL_TRACE_ID, SCOL_START_NANO, tag_metadata, TraceBlockStats, trace_bloom};

/// Merge existing span blocks with a caller-supplied on-disk read limit.
///
/// # Errors
/// Returns an error when an input exceeds the configured cap, the query is
/// malformed, an expression has incompatible operand types, or the backing
/// span store fails.
pub async fn compact_block_keys_with_max_bytes(
    store: Arc<dyn ObjectStore>,
    writer: &BlockWriter,
    index: &mut TraceIndex,
    tenant: &str,
    input_keys: &[String],
    output_key: &str,
    block_read_max: ByteSize,
) -> Result<BlockMeta, TracesError> {
    let mut batches = Vec::new();
    for key in input_keys {
        batches.extend(
            read_block_with_max_bytes(store.clone(), key, block_read_max)
                .await
                .map_err(|err| TracesError::Block(err.to_string()))?,
        );
    }

    if batches.is_empty() {
        return Err(TracesError::Block("cannot compact empty block set".into()));
    }

    let schema = span_block_schema();
    let concatenated =
        concat_batches(&schema, &batches).map_err(|err| TracesError::Block(err.to_string()))?;
    let concatenated = recompute_nested_sets(&concatenated)?;
    let concatenated = recompute_trace_level_columns(&concatenated)?;
    let meta = writer
        .write_block_with_decl(
            tenant,
            output_key,
            schema,
            std::slice::from_ref(&concatenated),
            &span_block_decl(),
            SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
        )
        .await
        .map_err(|err| TracesError::Block(err.to_string()))?;

    let compacted_batches = std::slice::from_ref(&concatenated);
    let (tag_names, tag_values) = tag_metadata(compacted_batches)?;
    index.replace_trace_blocks(
        tenant,
        input_keys,
        TraceBlockStats {
            object_key: meta.object_key.clone(),
            min_ts: meta.min_ts,
            max_ts: meta.max_ts,
            bloom: trace_bloom(compacted_batches)?,
            tag_names,
            tag_values,
        },
    );

    Ok(meta)
}
