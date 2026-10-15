use super::*;

struct TraceBatch {
    trace_id: [u8; 16],
    batch: RecordBatch,
}

pub(crate) async fn build_blocks_with_options(
    writer: &BlockWriter,
    index: &mut TraceIndex,
    tenant: &str,
    partition: i32,
    records: &[SpanRecord],
    offset_range: OffsetRange,
    options: BlockBuildOptions<'_>,
) -> Result<Vec<BlockMeta>, TracesError> {
    let mut grouped = group_by_trace(records);
    let mut per_trace: Vec<TraceBatch> = Vec::new();
    let mut tag_names = BTreeSet::new();
    let mut tag_values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut window_start_ns = i64::MAX;

    while let Some(identity) = grouped.keys().next().cloned() {
        let spans = grouped
            .remove(&identity)
            .expect("the trace identity came from this map");
        if identity.tenant != tenant {
            continue;
        }
        window_start_ns =
            window_start_ns.min(spans.iter().map(|span| span.start_ns).min().unwrap_or(0));
        collect_tags(&spans, &mut tag_names, &mut tag_values);
        per_trace.push(TraceBatch {
            trace_id: identity.trace_id,
            batch: span_batch_with_promoted_attrs(&spans, options.promoted_attrs)?,
        });
    }

    if per_trace.is_empty() {
        return Ok(Vec::new());
    }

    // The block schema declares `[trace_id, start_unix_nano]` as its sort key,
    // and the block-level trace-id bounds are only meaningful if the rows
    // really are in that order. `group_by_trace` sorts each trace's spans by
    // start, so ordering the per-trace batches by trace id orders the block.
    // Doing it here rather than leaning on the grouping map's key order keeps
    // the invariant a property of this function.
    per_trace.sort_by_key(|trace| trace.trace_id);
    let traces = per_trace
        .iter()
        .map(|trace| trace.trace_id)
        .collect::<Vec<_>>();
    let batches = per_trace
        .into_iter()
        .map(|trace| trace.batch)
        .collect::<Vec<_>>();

    let schema = span_block_schema_with_promoted_attrs(options.promoted_attrs);
    let concatenated =
        concat_batches(&schema, &batches).map_err(|err| TracesError::Block(err.to_string()))?;
    let key = object_key(
        tenant,
        partition,
        MinOffset(offset_range.min),
        MaxOffset(offset_range.max),
        WindowStartNs(window_start_ns),
    );
    let key = prefixed_object_key(options.object_key_prefix, &key);
    let meta = writer
        .write_block_with_decl(
            tenant,
            &key,
            schema,
            &[concatenated],
            &span_block_decl(),
            SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
        )
        .await
        .map_err(|err| TracesError::Block(err.to_string()))?;

    let mut bloom = ShardedTraceBloom::with_tempo_defaults(traces.len());
    for trace_id in traces {
        bloom.insert(&trace_id);
    }
    index.add_trace_block(
        tenant,
        TraceBlockStats {
            object_key: meta.object_key.clone(),
            min_ts: meta.min_ts,
            max_ts: meta.max_ts,
            bloom,
            tag_names,
            tag_values,
            row_count: meta.row_count,
            level: meta.level,
        },
    );

    Ok(vec![meta])
}


/// Build and write one span block, with an object-store prefix on its key.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub async fn build_blocks_with_prefix(
    writer: &BlockWriter,
    index: &mut TraceIndex,
    object_key_prefix: &str,
    tenant: &str,
    partition: i32,
    records: &[SpanRecord],
    offset_range: OffsetRange,
) -> Result<Vec<BlockMeta>, TracesError> {
    build_blocks_with_options(
        writer,
        index,
        tenant,
        partition,
        records,
        offset_range,
        BlockBuildOptions {
            object_key_prefix,
            promoted_attrs: &[],
        },
    )
    .await
}


///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub async fn build_blocks_with_promoted_attrs(
    writer: &BlockWriter,
    index: &mut TraceIndex,
    tenant: &str,
    partition: i32,
    records: &[SpanRecord],
    offset_range: OffsetRange,
    promoted_attrs: &[PromotedSpanAttr],
) -> Result<Vec<BlockMeta>, TracesError> {
    build_blocks_with_options(
        writer,
        index,
        tenant,
        partition,
        records,
        offset_range,
        BlockBuildOptions {
            object_key_prefix: "",
            promoted_attrs,
        },
    )
    .await
}
