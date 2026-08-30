use super::{Span, NestedSet, HashMap};

/// Assign modified pre-order traversal intervals to spans of one trace.
#[must_use]
pub fn assign_nested_set(spans: &[Span]) -> Vec<NestedSet> {
    enum Frame {
        Enter { idx: usize, parent_left: i32 },
        Exit { idx: usize },
    }

    let pos: HashMap<[u8; 8], usize> = spans
        .iter()
        .enumerate()
        .map(|(idx, span)| (span.span_id, idx))
        .collect();
    let mut children = vec![Vec::new(); spans.len()];
    let mut roots = Vec::new();

    for (idx, span) in spans.iter().enumerate() {
        match span
            .parent_span_id
            .and_then(|parent| pos.get(&parent).copied())
        {
            Some(parent_idx) if parent_idx != idx => children[parent_idx].push(idx),
            _ => roots.push(idx),
        }
    }

    let mut out = vec![NestedSet::default(); spans.len()];
    let mut counter = 1_i32;
    let mut stack = Vec::new();
    for &root in roots.iter().rev() {
        stack.push(Frame::Enter {
            idx: root,
            // Root spans have no parent. Tempo encodes this as nestedSetParent
            // = -1 (left values start at 1, so -1 never collides with a real
            // parent's left). Grafana's Traces Drilldown selects root spans with
            // the primary signal `nestedSetParent < 0`, so roots MUST be < 0.
            parent_left: -1,
        });
    }

    while let Some(frame) = stack.pop() {
        match frame {
            Frame::Enter { idx, parent_left } => {
                let left = counter;
                counter += 1;
                out[idx].left = left;
                out[idx].parent_id = parent_left;
                stack.push(Frame::Exit { idx });
                for &child in children[idx].iter().rev() {
                    stack.push(Frame::Enter {
                        idx: child,
                        parent_left: left,
                    });
                }
            }
            Frame::Exit { idx } => {
                out[idx].right = counter;
                counter += 1;
            }
        }
    }

    out
}
