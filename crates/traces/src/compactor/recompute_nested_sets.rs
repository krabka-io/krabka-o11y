use super::*;

pub(crate) fn recompute_nested_sets(batch: &RecordBatch) -> Result<RecordBatch, TracesError> {
    enum Frame {
        Enter { row: usize, parent_left: i32 },
        Exit { row: usize },
    }

    let trace_ids = fixed_column(batch, SCOL_TRACE_ID, 16)?;
    let span_ids = fixed_column(batch, SCOL_SPAN_ID, 8)?;
    let parent_span_ids = fixed_column(batch, SCOL_PARENT_SPAN_ID, 8)?;
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
    // Default to the root sentinel (-1, Tempo's no-parent value): a row not
    // reached by the per-trace DFS has no parent. 0 is an invalid parent (left
    // values start at 1).
    let mut parent_id = vec![-1_i32; batch.num_rows()];

    for rows in by_trace.values() {
        let mut pos = HashMap::new();
        for row in rows {
            if span_ids.is_null(*row) {
                continue;
            }
            let mut span_id = [0_u8; 8];
            span_id.copy_from_slice(span_ids.value(*row));
            pos.insert(span_id, *row);
        }

        let mut children: HashMap<usize, Vec<usize>> = HashMap::new();
        let mut roots = Vec::new();
        for row in rows {
            let parent = (!parent_span_ids.is_null(*row)).then(|| {
                let mut parent = [0_u8; 8];
                parent.copy_from_slice(parent_span_ids.value(*row));
                parent
            });
            match parent.and_then(|parent| pos.get(&parent).copied()) {
                Some(parent_row) if parent_row != *row => {
                    children.entry(parent_row).or_default().push(*row);
                }
                _ => roots.push(*row),
            }
        }

        let mut counter = 1_i32;
        let mut stack = Vec::new();
        for row in roots.iter().rev() {
            stack.push(Frame::Enter {
                row: *row,
                // Root span: nestedSetParent = -1 (Tempo no-parent sentinel).
                parent_left: -1,
            });
        }
        while let Some(frame) = stack.pop() {
            match frame {
                Frame::Enter {
                    row,
                    parent_left: parent,
                } => {
                    left[row] = counter;
                    parent_id[row] = parent;
                    counter += 1;
                    stack.push(Frame::Exit { row });
                    if let Some(children) = children.get(&row) {
                        for child in children.iter().rev() {
                            stack.push(Frame::Enter {
                                row: *child,
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

    let child_count = left
        .iter()
        .map(|node_left| {
            i32::try_from(
                parent_id
                    .iter()
                    .filter(|parent| *parent == node_left)
                    .count(),
            )
            .unwrap_or(i32::MAX)
        })
        .collect::<Vec<_>>();
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
