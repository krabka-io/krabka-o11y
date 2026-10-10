use std::collections::{BTreeMap, HashMap};

use arrow::array::{Array, FixedSizeBinaryArray};

/// Nested-set numbering for the rows of one span batch. Each trace is
/// numbered on its own, so `left` restarts at 1 for every trace.
pub(crate) struct BatchNestedSets {
    pub left: Vec<i32>,
    pub right: Vec<i32>,
    /// The parent's `left`, or -1 (Tempo's no-parent value) for a root and for
    /// a row the walk never reaches.
    pub parent_id: Vec<i32>,
    /// Each parent row's direct children within its own trace.
    pub children: BTreeMap<usize, Vec<usize>>,
}

/// The id columns of one span batch.
#[derive(Clone, Copy)]
pub(crate) struct SpanIdColumns<'a> {
    pub trace: &'a FixedSizeBinaryArray,
    pub span: &'a FixedSizeBinaryArray,
    pub parent_span: &'a FixedSizeBinaryArray,
}

/// Number the rows of a batch from its id columns, walking each trace's
/// parent links from its roots.
pub(crate) fn batch_nested_sets(columns: SpanIdColumns<'_>) -> BatchNestedSets {
    enum Frame {
        Enter { row: usize, parent_left: i32 },
        Exit { row: usize },
    }

    let SpanIdColumns {
        trace: trace_ids,
        span: span_ids,
        parent_span: parent_span_ids,
    } = columns;

    let num_rows = trace_ids.len();
    let mut by_trace: BTreeMap<[u8; 16], Vec<usize>> = BTreeMap::new();
    for row in 0..num_rows {
        if trace_ids.is_null(row) {
            continue;
        }
        let mut trace_id = [0_u8; 16];
        trace_id.copy_from_slice(trace_ids.value(row));
        by_trace.entry(trace_id).or_default().push(row);
    }

    let mut sets = BatchNestedSets {
        left: vec![0_i32; num_rows],
        right: vec![0_i32; num_rows],
        // 0 would be an invalid parent: left values start at 1.
        parent_id: vec![-1_i32; num_rows],
        children: BTreeMap::new(),
    };

    for rows in by_trace.values() {
        let mut positions = HashMap::new();
        for &row in rows {
            if span_ids.is_null(row) {
                continue;
            }
            let mut span_id = [0_u8; 8];
            span_id.copy_from_slice(span_ids.value(row));
            positions.insert(span_id, row);
        }

        let mut roots = Vec::new();
        for &row in rows {
            let parent = (!parent_span_ids.is_null(row)).then(|| {
                let mut parent = [0_u8; 8];
                parent.copy_from_slice(parent_span_ids.value(row));
                parent
            });
            match parent.and_then(|parent| positions.get(&parent).copied()) {
                Some(parent_row) if parent_row != row => {
                    sets.children.entry(parent_row).or_default().push(row);
                }
                _ => roots.push(row),
            }
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
                    sets.left[row] = counter;
                    sets.parent_id[row] = parent_left;
                    counter += 1;
                    stack.push(Frame::Exit { row });
                    if let Some(children) = sets.children.get(&row) {
                        for &child in children.iter().rev() {
                            stack.push(Frame::Enter {
                                row: child,
                                parent_left: sets.left[row],
                            });
                        }
                    }
                }
                Frame::Exit { row } => {
                    sets.right[row] = counter;
                    counter += 1;
                }
            }
        }
    }
    sets
}
