use super::{
    COL_CHILD_COUNT, COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID, COL_PARENT_SPAN_ID, COL_SPAN_ID,
    COL_TRACE_ID, RecordBatch, TraceqlError, fixed, replace_scan_int32_columns,
    replace_scan_int32_columns::ScanInt32ColumnReplacement,
};
use crate::span::nested_set::{BatchNestedSets, SpanIdColumns, batch_nested_sets};

pub(crate) fn recompute_batch_nested_sets(
    batch: &RecordBatch,
) -> Result<RecordBatch, TraceqlError> {
    let BatchNestedSets {
        left,
        right,
        parent_id,
        children,
    } = batch_nested_sets(SpanIdColumns {
        trace: fixed(batch, COL_TRACE_ID)?,
        span: fixed(batch, COL_SPAN_ID)?,
        parent_span: fixed(batch, COL_PARENT_SPAN_ID)?,
    });
    // childCount is PER TRACE: each parent's direct children, scoped to this
    // trace's rows. The nested-set `left` values reset to 1 per trace, so a
    // batch-global count would collide across traces and over-count.
    let mut child_count = vec![0_i32; batch.num_rows()];
    for entry in &children {
        let parent_row = entry.0;
        let kids = entry.1;
        child_count[*parent_row] = i32::try_from(kids.len()).unwrap_or(i32::MAX);
    }

    replace_scan_int32_columns(
        batch,
        &[
            ScanInt32ColumnReplacement {
                name: COL_NS_LEFT,
                values: left,
            },
            ScanInt32ColumnReplacement {
                name: COL_NS_RIGHT,
                values: right,
            },
            ScanInt32ColumnReplacement {
                name: COL_PARENT_ID,
                values: parent_id,
            },
            ScanInt32ColumnReplacement {
                name: COL_CHILD_COUNT,
                values: child_count,
            },
        ],
    )
}
