use super::{
    Array, BTreeMap, COL_CHILD_COUNT, COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID, COL_PARENT_SPAN_ID,
    COL_SPAN_ID, COL_TRACE_ID, RecordBatch, TraceqlError, fixed, replace_scan_int32_columns,
};

pub(crate) fn recompute_batch_nested_sets(
    batch: &RecordBatch,
) -> Result<RecordBatch, TraceqlError> {
    enum Frame {
        Enter { row: usize, parent_left: i32 },
        Exit { row: usize },
    }

    let trace_ids = fixed(batch, COL_TRACE_ID)?;
    let span_ids = fixed(batch, COL_SPAN_ID)?;
    let parent_span_ids = fixed(batch, COL_PARENT_SPAN_ID)?;
    let mut by_trace: BTreeMap<[u8; 16], Vec<usize>> = BTreeMap::new();
    for row in 0..batch.num_rows() {
        if trace_ids.is_null(row) {
            continue;
        }
        let mut trace_id = [0_u8; 16];
        trace_id.copy_from_slice(trace_ids.value(row));
        by_trace.entry(trace_id).or_default().push(row);
    }

    let mut left = vec![0_i32; batch.num_rows()];
    let mut right = vec![0_i32; batch.num_rows()];
    // Default to the root sentinel (-1): a row not reached by the per-trace DFS
    // (e.g. a null trace_id) has no parent. 0 would be an invalid parent (left
    // values start at 1) and would read as "has a parent at left 0".
    let mut parent_id = vec![-1_i32; batch.num_rows()];
    let mut child_count = vec![0_i32; batch.num_rows()];

    for rows in by_trace.values() {
        let mut positions = BTreeMap::new();
        for &row in rows {
            if span_ids.is_null(row) {
                continue;
            }
            let mut span_id = [0_u8; 8];
            span_id.copy_from_slice(span_ids.value(row));
            positions.insert(span_id, row);
        }

        let mut children = BTreeMap::<usize, Vec<usize>>::new();
        let mut roots = Vec::new();
        for &row in rows {
            let parent = (!parent_span_ids.is_null(row)).then(|| {
                let mut parent = [0_u8; 8];
                parent.copy_from_slice(parent_span_ids.value(row));
                parent
            });
            match parent.and_then(|parent| positions.get(&parent).copied()) {
                Some(parent_row) if parent_row != row => {
                    children.entry(parent_row).or_default().push(row);
                }
                _ => roots.push(row),
            }
        }

        // childCount is PER TRACE: each parent's direct children, scoped to this
        // trace's rows. The nested-set `left` values reset to 1 per trace, so a
        // batch-global count would collide across traces and over-count.
        for (&parent_row, kids) in &children {
            child_count[parent_row] = i32::try_from(kids.len()).unwrap_or(i32::MAX);
        }

        let mut counter = 1_i32;
        let mut stack = Vec::new();
        for &row in roots.iter().rev() {
            stack.push(Frame::Enter {
                row,
                // Root span: nestedSetParent = -1 (Tempo no-parent sentinel).
                parent_left: -1,
            });
        }
        while let Some(frame) = stack.pop() {
            match frame {
                Frame::Enter { row, parent_left } => {
                    left[row] = counter;
                    parent_id[row] = parent_left;
                    counter += 1;
                    stack.push(Frame::Exit { row });
                    if let Some(children) = children.get(&row) {
                        for &child in children.iter().rev() {
                            stack.push(Frame::Enter {
                                row: child,
                                parent_left: left[row],
                            });
                        }
                    }
                }
                Frame::Exit { row } => {
                    right[row] = counter;
                    counter += 1;
                }
            }
        }
    }

    replace_scan_int32_columns(
        batch,
        &[
            (COL_NS_LEFT, left),
            (COL_NS_RIGHT, right),
            (COL_PARENT_ID, parent_id),
            (COL_CHILD_COUNT, child_count),
        ],
    )
}
