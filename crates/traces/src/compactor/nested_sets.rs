use super::*;
use crate::span::nested_set::{BatchNestedSets, SpanIdColumns, batch_nested_sets};

pub(crate) struct Int32ColumnReplacement<'a> {
    pub(crate) name: &'a str,
    pub(crate) values: Vec<i32>,
}

pub(crate) fn replace_int32_columns(
    batch: &RecordBatch,
    replacements: &[Int32ColumnReplacement<'_>],
) -> Result<RecordBatch, TracesError> {
    let schema = batch.schema();
    let mut columns = batch.columns().to_vec();
    for replacement in replacements {
        let Int32ColumnReplacement { name, values } = replacement;
        let idx = schema
            .column_with_name(name)
            .ok_or_else(|| TracesError::Block(format!("missing column {name}")))?
            .0;
        columns[idx] = Arc::new(Int32Array::from(values.clone()));
    }
    RecordBatch::try_new(schema, columns).map_err(|err| TracesError::Block(err.to_string()))
}

pub(crate) fn recompute_nested_sets(batch: &RecordBatch) -> Result<RecordBatch, TracesError> {
    let BatchNestedSets {
        left,
        right,
        parent_id,
        children,
    } = batch_nested_sets(SpanIdColumns {
        trace: fixed_column(batch, SCOL_TRACE_ID, 16)?,
        span: fixed_column(batch, SCOL_SPAN_ID, 8)?,
        parent_span: fixed_column(batch, SCOL_PARENT_SPAN_ID, 8)?,
    });
    // Only a parent the walk reaches is credited with its children, and each
    // trace's children are its own: the `left` numbering restarts at 1 for
    // every trace, so counting from it would credit one trace's root with
    // another's children.
    let mut child_count = vec![0_i32; batch.num_rows()];
    for &parent_row in children.keys() {
        if left[parent_row] != 0 {
            let child_rows = children
                .get(&parent_row)
                .expect("the parent row came from the child map");
            child_count[parent_row] = i32::try_from(child_rows.len()).unwrap_or(i32::MAX);
        }
    }

    replace_int32_columns(
        batch,
        &[
            Int32ColumnReplacement {
                name: SCOL_NESTED_SET_LEFT,
                values: left,
            },
            Int32ColumnReplacement {
                name: SCOL_NESTED_SET_RIGHT,
                values: right,
            },
            Int32ColumnReplacement {
                name: SCOL_PARENT_ID,
                values: parent_id,
            },
            Int32ColumnReplacement {
                name: SCOL_CHILD_COUNT,
                values: child_count,
            },
        ],
    )
}

struct RootNames {
    service: Option<String>,
    span_name: Option<String>,
}

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
pub(crate) fn recompute_trace_level_columns(
    batch: &RecordBatch,
) -> Result<RecordBatch, TracesError> {
    let trace_ids = fixed_column(batch, SCOL_TRACE_ID, 16)?;
    let parent_span_ids = fixed_column(batch, SCOL_PARENT_SPAN_ID, 8)?;
    let start = int64_column(batch, SCOL_START_NANO)?;
    let duration = int64_column(batch, SCOL_DURATION_NANOS)?;
    let name = string_column(batch, SCOL_NAME)?;
    let root_service = string_column(batch, SCOL_ROOT_SERVICE_NAME)?;

    let by_trace = crate::group_rows_by_trace_id::group_rows_by_trace_id(trace_ids);

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
        let root_names = root_row.map_or(
            RootNames {
                service: None,
                span_name: None,
            },
            |row| RootNames {
                service: (!root_service.is_null(row)).then(|| root_service.value(row).to_string()),
                span_name: (!name.is_null(row)).then(|| name.value(row).to_string()),
            },
        );

        for &row in rows {
            trace_start[row] = min_start;
            trace_duration[row] = dur;
            root_service_out[row].clone_from(&root_names.service);
            root_name_out[row].clone_from(&root_names.span_name);
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
