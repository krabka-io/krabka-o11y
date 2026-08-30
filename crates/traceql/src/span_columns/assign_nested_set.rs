use super::*;

#[must_use]
pub fn assign_nested_set(spans: &[InputSpan]) -> Vec<NestedSet> {
    enum Frame {
        Enter { idx: usize, parent_left: i32 },
        Exit { idx: usize },
    }

    let pos: HashMap<[u8; 8], usize> = spans
        .iter()
        .enumerate()
        .map(|(i, span)| (span.span_id, i))
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
            left: 0,
            right: 0,
            parent_id: 0,
        };
        spans.len()
    ];
    let mut counter = 1_i32;
    let mut stack = Vec::new();

    for &root in roots.iter().rev() {
        stack.push(Frame::Enter {
            idx: root,
            // Root spans encode nestedSetParent = -1 (Tempo's no-parent
            // sentinel; left values start at 1 so -1 never collides). Grafana's
            // Traces Drilldown selects roots with `nestedSetParent < 0`.
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

    // Spans caught in a parent cycle are excluded from `roots` (neither absent
    // nor self-parented), so the root-seeded DFS never reaches them and leaves
    // them at {left:0,right:0}, which would collide with real roots. Sweep for
    // any still-unassigned span and seed it as an additional root.
    while let Some(start) = out.iter().position(|bounds| bounds.left == 0) {
        stack.push(Frame::Enter {
            idx: start,
            // Cycle-orphaned spans become additional roots: same -1 sentinel.
            parent_left: -1,
        });
        while let Some(frame) = stack.pop() {
            match frame {
                Frame::Enter { idx, parent_left } => {
                    let left = counter;
                    counter += 1;
                    out[idx].left = left;
                    out[idx].parent_id = parent_left;
                    stack.push(Frame::Exit { idx });
                    for &child in children[idx].iter().rev() {
                        if out[child].left == 0 {
                            stack.push(Frame::Enter {
                                idx: child,
                                parent_left: left,
                            });
                        }
                    }
                }
                Frame::Exit { idx } => {
                    out[idx].right = counter;
                    counter += 1;
                }
            }
        }
    }

    out
}
