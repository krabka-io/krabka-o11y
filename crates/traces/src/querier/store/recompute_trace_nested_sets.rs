use super::{SpanRef, BTreeMap, Array};

pub(crate) fn recompute_trace_nested_sets(spans: &mut [SpanRef]) {
    enum Frame {
        Enter { idx: usize, parent_left: i32 },
        Exit { idx: usize },
    }

    let positions = spans
        .iter()
        .enumerate()
        .map(|(idx, span)| (span.span_id, idx))
        .collect::<BTreeMap<_, _>>();
    let mut children = vec![Vec::new(); spans.len()];
    let mut roots = Vec::new();
    for (idx, span) in spans.iter().enumerate() {
        match span
            .parent_span_id
            .and_then(|parent| positions.get(&parent).copied())
        {
            Some(parent_idx) if parent_idx != idx => children[parent_idx].push(idx),
            _ => roots.push(idx),
        }
    }

    let mut counter = 1_i32;
    let mut stack = Vec::new();
    for &root in roots.iter().rev() {
        stack.push(Frame::Enter {
            idx: root,
            // Root spans encode nestedSetParent = -1 (Tempo's no-parent sentinel;
            // left values start at 1 so -1 never collides). Must match the stored
            // assignment in span/nested_set.rs and the Drilldown's `< 0` signal.
            parent_left: -1,
        });
    }
    while let Some(frame) = stack.pop() {
        match frame {
            Frame::Enter { idx, parent_left } => {
                let left = counter;
                counter += 1;
                spans[idx].nested_set_left = left;
                spans[idx].nested_set_parent = parent_left;
                stack.push(Frame::Exit { idx });
                for &child in children[idx].iter().rev() {
                    stack.push(Frame::Enter {
                        idx: child,
                        parent_left: left,
                    });
                }
            }
            Frame::Exit { idx } => {
                spans[idx].nested_set_right = counter;
                counter += 1;
            }
        }
    }
}
