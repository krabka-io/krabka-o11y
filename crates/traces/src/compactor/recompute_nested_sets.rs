use super::{
    RecordBatch, SCOL_CHILD_COUNT, SCOL_NESTED_SET_LEFT, SCOL_NESTED_SET_RIGHT, SCOL_PARENT_ID,
    SCOL_PARENT_SPAN_ID, SCOL_SPAN_ID, SCOL_TRACE_ID, TracesError, fixed_column,
    replace_int32_columns,
};
use crate::span::nested_set::{BatchNestedSets, batch_nested_sets};

pub(crate) fn recompute_nested_sets(batch: &RecordBatch) -> Result<RecordBatch, TracesError> {
    let BatchNestedSets {
        left,
        right,
        parent_id,
        children,
    } = batch_nested_sets(
        fixed_column(batch, SCOL_TRACE_ID, 16)?,
        fixed_column(batch, SCOL_SPAN_ID, 8)?,
        fixed_column(batch, SCOL_PARENT_SPAN_ID, 8)?,
    );
    // Only a parent the walk reaches is credited with its children, and each
    // trace's children are its own: the `left` numbering restarts at 1 for
    // every trace, so counting from it would credit one trace's root with
    // another's children.
    let mut child_count = vec![0_i32; batch.num_rows()];
    for (&parent_row, kids) in &children {
        if left[parent_row] != 0 {
            child_count[parent_row] = i32::try_from(kids.len()).unwrap_or(i32::MAX);
        }
    }

    replace_int32_columns(
        batch,
        &[
            (SCOL_NESTED_SET_LEFT, left),
            (SCOL_NESTED_SET_RIGHT, right),
            (SCOL_PARENT_ID, parent_id),
            (SCOL_CHILD_COUNT, child_count),
        ],
    )
}
