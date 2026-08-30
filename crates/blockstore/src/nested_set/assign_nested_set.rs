use super::*;

/// Assigns nested-set intervals by DFS preorder over the trace forest.
#[must_use]
pub fn assign_nested_set(spans: &[SpanNode]) -> Vec<NestedSet> {
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
            // -1 is Tempo's no-parent (root) sentinel; left values start at 1 so
            // it never collides with a real parent's left.
            nested_set_right: 0,
            parent_id: -1,
        };
        spans.len()
    ];
    let mut counter = 1_i32;
    let mut visited = vec![false; spans.len()];

    // DFS from the discovered roots. Then sweep for any span the DFS never
    // reached — under cyclic/garbage parentage (e.g. A.parent=B, B.parent=A)
    // a node can be neither a root nor a descendant of one, so it would keep
    // `{0, 0, 0}` and collide with real roots. Seed each such span as an
    // additional root so every node gets a valid `left < right` interval.
    let mut stack = Vec::new();
    for root in roots.iter().copied().chain(0..spans.len()) {
        stack.push(Frame::Enter {
            idx: root,
            // Root span, or cycle-orphaned span re-seeded as a root:
            // nestedSetParent = -1 (Tempo no-parent sentinel).
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
