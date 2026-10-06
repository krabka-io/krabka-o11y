use super::*;

pub(crate) fn assemble_search_response(
    batches: &[RecordBatch],
    limit: usize,
    spss: usize,
    most_recent: bool,
    inspected: ByteSize,
) -> Result<SearchResponse> {
    let mut traces: BTreeMap<[u8; 16], TraceAcc> = BTreeMap::new();
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let trace_id = batch.column_by_name(COL_TRACE_ID);
        let span_id = batch.column_by_name(COL_SPAN_ID);
        let parent_span_id = batch.column_by_name(COL_PARENT_SPAN_ID);
        let name = batch.column_by_name(COL_NAME);
        let kind = batch.column_by_name(COL_KIND);
        let ns_left = batch.column_by_name(COL_NS_LEFT);
        let ns_right = batch.column_by_name(COL_NS_RIGHT);
        let parent_id = batch.column_by_name(COL_PARENT_ID);
        let start = batch.column_by_name(COL_START);
        let duration = batch.column_by_name(COL_DURATION);
        let status_code = batch.column_by_name(COL_STATUS_CODE);
        let status_message = batch.column_by_name(COL_STATUS_MESSAGE);
        let instrumentation_name = batch.column_by_name(COL_INSTRUMENTATION_NAME);
        let instrumentation_version = batch.column_by_name(COL_INSTRUMENTATION_VERSION);
        let root_service_name = batch.column_by_name(COL_ROOT_SERVICE_NAME);
        let root_span_name = batch.column_by_name(COL_ROOT_SPAN_NAME);
        let trace_start = batch.column_by_name(COL_TRACE_START);
        let trace_duration = batch.column_by_name(COL_TRACE_DURATION);
        for row in 0..batch.num_rows() {
            let trace_id = fixed_16_column(trace_id, COL_TRACE_ID, row)?;
            let span = SpanRef {
                span_id: fixed_8_column(span_id, COL_SPAN_ID, row)?,
                parent_span_id: optional_fixed_8_column(parent_span_id, COL_PARENT_SPAN_ID, row)?,
                name: string_value_column(name, row).unwrap_or_default(),
                kind: i32_value_column(kind, COL_KIND, row)?,
                nested_set_left: i32_value_column(ns_left, COL_NS_LEFT, row)?,
                nested_set_right: i32_value_column(ns_right, COL_NS_RIGHT, row)?,
                nested_set_parent: i32_value_column(parent_id, COL_PARENT_ID, row)?,
                start_time_unix_nano: u64_from_i64(i64_value_column(start, COL_START, row)?)?,
                duration: Time::from_nanos(i64_value_column(duration, COL_DURATION, row)?),
                status_code: i32_value_column(status_code, COL_STATUS_CODE, row)?,
                status_message: string_value_column(status_message, row).unwrap_or_default(),
                instrumentation_name: string_value_column(instrumentation_name, row)
                    .unwrap_or_default(),
                instrumentation_version: string_value_column(instrumentation_version, row)
                    .unwrap_or_default(),
                resource_attributes: Vec::new(),
                attributes: row_attrs(batch, row)?,
                events: Vec::new(),
                links: Vec::new(),
            };
            traces
                .entry(trace_id)
                .or_insert_with(|| TraceAcc {
                    root_service_name: string_value_column(root_service_name, row)
                        .unwrap_or_default(),
                    root_trace_name: string_value_column(root_span_name, row).unwrap_or_default(),
                    start_time_unix_nano: u64_from_i64(
                        i64_value_column(trace_start, COL_TRACE_START, row).unwrap_or_default(),
                    )
                    .unwrap_or_default(),
                    duration: Time::from_nanos(
                        i64_value_column(trace_duration, COL_TRACE_DURATION, row)
                            .unwrap_or_default(),
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
            let spans = acc.spans.into_iter().take(spss).collect();
            TraceResult {
                trace_id,
                root_service_name: acc.root_service_name,
                root_trace_name: acc.root_trace_name,
                start_time_unix_nano: acc.start_time_unix_nano,
                duration: acc.duration,
                span_sets: vec![SpanSet { spans, matched }],
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
