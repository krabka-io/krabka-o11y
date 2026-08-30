use super::{RecordBatch, TracesError, fixed_column, SCOL_TRACE_ID, SCOL_PARENT_SPAN_ID, int64_column, SCOL_START_NANO, SCOL_DURATION_NANOS, string_column, SCOL_NAME, SCOL_ROOT_SERVICE_NAME, BTreeMap, Array, set_column, SCOL_TRACE_START_NANO, Arc, Int64Array, SCOL_TRACE_DURATION_NANOS, StringArray, SCOL_ROOT_SPAN_NAME};

/// Recompute the trace-level denormalized columns over the FULL merged trace.
///
/// The write path in `span/batch.rs::root_info` sets `trace_start_unix_nano`,
/// `trace_duration_nanos`, `root_service_name` and `root_span_name` from only
/// the spans in one flush-window block.
///
/// After a compaction of several blocks of the same trace, each origin block's
/// rows still carry that block's partial and stale values. The trace-level
/// `TraceQL` matchers `trace:duration`, `trace:rootName` and
/// `trace:rootService` would then read wrong data. This function regroups by
/// `trace_id` and recomputes the four columns across all merged rows.
pub(crate) fn recompute_trace_level_columns(batch: &RecordBatch) -> Result<RecordBatch, TracesError> {
    let trace_ids = fixed_column(batch, SCOL_TRACE_ID, 16)?;
    let parent_span_ids = fixed_column(batch, SCOL_PARENT_SPAN_ID, 8)?;
    let start = int64_column(batch, SCOL_START_NANO)?;
    let duration = int64_column(batch, SCOL_DURATION_NANOS)?;
    let name = string_column(batch, SCOL_NAME)?;
    let root_service = string_column(batch, SCOL_ROOT_SERVICE_NAME)?;

    let mut by_trace: BTreeMap<[u8; 16], Vec<usize>> = BTreeMap::new();
    for row in 0..batch.num_rows() {
        if trace_ids.is_null(row) {
            continue;
        }
        let mut trace_id = [0_u8; 16];
        trace_id.copy_from_slice(trace_ids.value(row));
        by_trace.entry(trace_id).or_default().push(row);
    }

    let rows_n = batch.num_rows();
    let mut trace_start = vec![0_i64; rows_n];
    let mut trace_duration = vec![0_i64; rows_n];
    let mut root_service_out: Vec<Option<String>> = vec![None; rows_n];
    let mut root_name_out: Vec<Option<String>> = vec![None; rows_n];

    for rows in by_trace.values() {
        let mut min_start = i64::MAX;
        let mut max_end = i64::MIN;
        for &row in rows {
            let s = start.value(row);
            min_start = min_start.min(s);
            let d = if duration.is_null(row) {
                0
            } else {
                duration.value(row)
            };
            max_end = max_end.max(s.saturating_add(d));
        }
        let dur = max_end.saturating_sub(min_start).max(0);

        // Root = the first span with no in-trace parent, else the earliest span
        // (matching the write-path `root_info`). `root_service_name` of the root
        // row is its trace's root service; `name` is the root span's own name.
        let root_row = rows
            .iter()
            .copied()
            .find(|&row| parent_span_ids.is_null(row))
            .or_else(|| rows.iter().copied().min_by_key(|&row| start.value(row)));
        let (service, span_name) = root_row.map_or((None, None), |row| {
            (
                (!root_service.is_null(row)).then(|| root_service.value(row).to_string()),
                (!name.is_null(row)).then(|| name.value(row).to_string()),
            )
        });

        for &row in rows {
            trace_start[row] = min_start;
            trace_duration[row] = dur;
            root_service_out[row].clone_from(&service);
            root_name_out[row].clone_from(&span_name);
        }
    }

    let schema = batch.schema();
    let mut columns = batch.columns().to_vec();
    set_column(
        &schema,
        &mut columns,
        SCOL_TRACE_START_NANO,
        Arc::new(Int64Array::from(trace_start)),
    )?;
    set_column(
        &schema,
        &mut columns,
        SCOL_TRACE_DURATION_NANOS,
        Arc::new(Int64Array::from(trace_duration)),
    )?;
    set_column(
        &schema,
        &mut columns,
        SCOL_ROOT_SERVICE_NAME,
        Arc::new(root_service_out.into_iter().collect::<StringArray>()),
    )?;
    set_column(
        &schema,
        &mut columns,
        SCOL_ROOT_SPAN_NAME,
        Arc::new(root_name_out.into_iter().collect::<StringArray>()),
    )?;
    RecordBatch::try_new(schema, columns).map_err(|err| TracesError::Block(err.to_string()))
}
