use super::*;

pub(crate) fn assemble_search_response(
    batches: &[RecordBatch],
    limit: usize,
    spss: usize,
    most_recent: bool,
    inspected: ByteSize,
) -> Result<SearchResponse> {
    let mut traces: BTreeMap<[u8; 16], TraceAcc> = BTreeMap::new();
    let mut groups: BTreeMap<[u8; 16], BTreeMap<String, SpanSet>> = BTreeMap::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let trace_id = fixed_16(batch, COL_TRACE_ID, row)?;
            let span = SpanRef {
                span_id: fixed_8(batch, COL_SPAN_ID, row)?,
                parent_span_id: optional_fixed_8(batch, COL_PARENT_SPAN_ID, row)?,
                name: string_value(batch, COL_NAME, row).unwrap_or_default(),
                kind: i32_value(batch, COL_KIND, row)?,
                nested_set_left: i32_value(batch, COL_NS_LEFT, row)?,
                nested_set_right: i32_value(batch, COL_NS_RIGHT, row)?,
                nested_set_parent: i32_value(batch, COL_PARENT_ID, row)?,
                start_time_unix_nano: u64_from_i64(i64_value(batch, COL_START, row)?)?,
                duration: Time::from_nanos(i64_value(batch, COL_DURATION, row)?),
                status_code: i32_value(batch, COL_STATUS_CODE, row)?,
                status_message: string_value(batch, COL_STATUS_MESSAGE, row).unwrap_or_default(),
                instrumentation_name: string_value(batch, COL_INSTRUMENTATION_NAME, row)
                    .unwrap_or_default(),
                instrumentation_version: string_value(batch, COL_INSTRUMENTATION_VERSION, row)
                    .unwrap_or_default(),
                resource_attributes: Vec::new(),
                attributes: row_attrs(batch, row)?,
                events: Vec::new(),
                links: Vec::new(),
            };
            if let Some(group_id) = string_value(batch, "__traceql.spanset", row) {
                let group = groups
                    .entry(trace_id)
                    .or_default()
                    .entry(group_id)
                    .or_insert_with(|| SpanSet {
                        spans: Vec::new(),
                        matched: 0,
                        attributes: Vec::new(),
                    });
                if group.spans.is_empty()
                    && let Some(encoded) = string_value(batch, "__traceql.spanset_attributes", row)
                {
                    group.attributes = serde_json::from_str(&encoded)
                        .map_err(|error| TraceqlError::Exec(error.to_string()))?;
                }
                group.spans.push(span.clone());
            }
            traces
                .entry(trace_id)
                .or_insert_with(|| TraceAcc {
                    root_service_name: string_value(batch, COL_ROOT_SERVICE_NAME, row)
                        .unwrap_or_default(),
                    root_trace_name: string_value(batch, COL_ROOT_SPAN_NAME, row)
                        .unwrap_or_default(),
                    start_time_unix_nano: u64_from_i64(
                        i64_value(batch, COL_TRACE_START, row).unwrap_or_default(),
                    )
                    .unwrap_or_default(),
                    duration: Time::from_nanos(
                        i64_value(batch, COL_TRACE_DURATION, row).unwrap_or_default(),
                    ),
                    spans: Vec::new(),
                })
                .spans
                .push(span);
        }
    }

    let mut out: Vec<TraceResult> = traces
        .into_iter()
        .map(|(trace_id, mut acc)| {
            deduplicate_search_spans(&mut acc.spans);
            let matched = u32::try_from(acc.spans.len()).unwrap_or(u32::MAX);
            let grouped = groups
                .get(&trace_id)
                .into_iter()
                .flat_map(|groups| groups.values())
                .map(|group| {
                    let mut spans = group.spans.clone();
                    deduplicate_search_spans(&mut spans);
                    let matched = u32::try_from(spans.len()).unwrap_or(u32::MAX);
                    SpanSet {
                        spans: spans.into_iter().take(spss).collect(),
                        matched,
                        attributes: group.attributes.clone(),
                    }
                })
                .collect::<Vec<_>>();
            let span_sets = if grouped.is_empty() {
                vec![SpanSet {
                    spans: acc.spans.into_iter().take(spss).collect(),
                    matched,
                    attributes: Vec::new(),
                }]
            } else {
                grouped
            };
            TraceResult {
                trace_id,
                root_service_name: acc.root_service_name,
                root_trace_name: acc.root_trace_name,
                start_time_unix_nano: acc.start_time_unix_nano,
                duration: acc.duration,
                span_sets,
            }
        })
        .collect();
    let inspected_traces = out.len();
    if most_recent {
        out.sort_by(|a, b| {
            b.start_time_unix_nano
                .cmp(&a.start_time_unix_nano)
                .then_with(|| a.trace_id.cmp(&b.trace_id))
        });
    } else {
        out.sort_by_key(|t| (t.start_time_unix_nano, t.trace_id));
    }
    out.truncate(limit);
    Ok(SearchResponse {
        traces: out,
        inspected_traces,
        inspected,
    })
}
