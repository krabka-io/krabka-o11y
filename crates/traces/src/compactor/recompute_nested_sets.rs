use super::{
    RecordBatch, SCOL_CHILD_COUNT, SCOL_NESTED_SET_LEFT, SCOL_NESTED_SET_RIGHT, SCOL_PARENT_ID,
    SCOL_PARENT_SPAN_ID, SCOL_SPAN_ID, SCOL_TRACE_ID, TracesError, fixed_column,
    replace_int32_columns, replace_int32_columns::Int32ColumnReplacement,
};
use crate::span::nested_set::{BatchNestedSets, SpanIdColumns, batch_nested_sets};

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
