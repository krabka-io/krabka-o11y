use super::{BTreeMap, BTreeSet, BlockBuildOptions, BlockMeta, BlockWriter, MaxOffset, MinOffset, SCOL_START_NANO, SCOL_TRACE_ID, ShardedTraceBloom, SpanRecord, SummaryColumns, TraceBlockStats, TraceIndex, TracesError, WindowStartNs, collect_tags, concat_batches, group_by_trace, object_key, prefixed_object_key, span_batch_with_promoted_attrs, span_block_decl, span_block_schema_with_promoted_attrs};

pub(crate) async fn build_blocks_with_options(
    writer: &BlockWriter,
    index: &mut TraceIndex,
    tenant: &str,
    partition: i32,
    records: &[SpanRecord],
    offset_range: (i64, i64),
    options: BlockBuildOptions<'_>,
) -> Result<Vec<BlockMeta>, TracesError> {
    let grouped = group_by_trace(records);
    let mut batches = Vec::new();
    let mut traces = Vec::new();
    let mut tag_names = BTreeSet::new();
    let mut tag_values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut window_start_ns = i64::MAX;

    for ((record_tenant, trace_id), spans) in grouped {
        if record_tenant != tenant {
            continue;
        }
        window_start_ns =
            window_start_ns.min(spans.iter().map(|span| span.start_ns).min().unwrap_or(0));
        collect_tags(&spans, &mut tag_names, &mut tag_values);
        traces.push(trace_id);
        batches.push(span_batch_with_promoted_attrs(
            &spans,
            options.promoted_attrs,
        )?);
    }

    if batches.is_empty() {
        return Ok(Vec::new());
    }

    let schema = span_block_schema_with_promoted_attrs(options.promoted_attrs);
    let concatenated =
        concat_batches(&schema, &batches).map_err(|err| TracesError::Block(err.to_string()))?;
    let key = object_key(
        tenant,
        partition,
        MinOffset(offset_range.0),
        MaxOffset(offset_range.1),
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
        },
    );

    Ok(vec![meta])
}
