use super::{CycleSpans, HashMap, NestedSet, SpanNode};

/// Assigns nested-set intervals by DFS preorder over the trace forest.
///
/// Roots are numbered in span order. `cycle_spans` decides what happens to
/// spans that no root reaches because their parent links form a cycle.
#[must_use]
pub fn assign_nested_set(spans: &[SpanNode], cycle_spans: CycleSpans) -> Vec<NestedSet> {
    enum Frame {
        Enter { idx: usize, parent_left: i32 },
        Exit { idx: usize },
    }

    let pos: HashMap<[u8; 8], usize> = spans
        .iter()
        .enumerate()
        .map(|(i, s)| (s.span_id, i))
        .collect();

    let mut children = vec![Vec::new(); spans.len()];
    let mut roots = Vec::new();
    for (i, span) in spans.iter().enumerate() {
        match span.parent_span_id.and_then(|p| pos.get(&p).copied()) {
            Some(parent_idx) if parent_idx != i => children[parent_idx].push(i),
            _ => roots.push(i),
        }
    }

    let mut out = vec![
        NestedSet {
            nested_set_left: 0,
            nested_set_right: 0,
            parent_id: 0,
        };
        spans.len()
    ];
    let mut counter = 1_i32;
    let mut visited = vec![false; spans.len()];

    // DFS from the discovered roots. Under cyclic/garbage parentage (e.g.
    // A.parent=B, B.parent=A) a node can be neither a root nor a descendant of
    // one, so it would keep `{0, 0, 0}` and collide with real roots. With
    // `AssignIntervals`, sweep every span afterwards and seed each unreached
    // one as an additional root so every node gets a valid `left < right`
    // interval.
    let cycle_sweep = match cycle_spans {
        CycleSpans::AssignIntervals => 0..spans.len(),
        CycleSpans::LeaveUnassigned => 0..0,
    };
    let mut stack = Vec::new();
    for root in roots.iter().copied().chain(cycle_sweep) {
        stack.push(Frame::Enter {
            idx: root,
            // Root span, or cycle-orphaned span re-seeded as a root:
            // nestedSetParent = -1 (Tempo's no-parent sentinel; left values
            // start at 1 so it never collides with a real parent's left).
            parent_left: -1,
        });
        while let Some(frame) = stack.pop() {
            match frame {
                Frame::Enter { idx, parent_left } => {
                    if visited[idx] {
                        continue;
                    }
                    visited[idx] = true;
                    let left = counter;
                    counter += 1;
                    out[idx].nested_set_left = left;
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
                    out[idx].nested_set_right = counter;
                    counter += 1;
                }
            }
        }
    }

    out
}
