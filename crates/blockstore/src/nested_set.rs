//! Nested-set interval assignment for trace span forests.

use std::collections::HashMap;

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, time::Duration};

    use super::*;

    fn sid(n: u8) -> [u8; 8] {
        [n, 0, 0, 0, 0, 0, 0, 0]
    }

    /// A span naming itself as its own parent is a root, not its own child.
    /// The `parent_idx != i` guard is what decides that; forced true, the span
    /// is pushed into its own children list and the traversal descends into a
    /// cycle instead of laying out a tree.
    #[test]
    fn a_self_parenting_span_is_treated_as_a_root() {
        let spans = vec![
            SpanNode {
                span_id: sid(1),
                parent_span_id: Some(sid(1)),
            },
            SpanNode {
                span_id: sid(2),
                parent_span_id: Some(sid(1)),
            },
        ];

        let sets = assign_nested_set(&spans);
        assert2::check!(sets.len() == 2);
        // The self-parenting span encloses its real child.
        assert2::check!(sets[0].nested_set_left < sets[1].nested_set_left);
        assert2::check!(sets[1].nested_set_right < sets[0].nested_set_right);
    }

    /// The self-parenting span has to share the forest with an unrelated root
    /// for the guard to be observable.
    ///
    /// With only its own child beside it, dropping the guard produces the same
    /// intervals: the span falls out of `roots`, but the `chain(0..len)` sweep
    /// that exists for cycle-orphaned spans picks it up again in the same
    /// order. Put a real root next to it and the orders diverge -- the sweep
    /// reaches the self-parent *after* that root, so the two swap intervals,
    /// and `nestedSetParent` stops matching the enclosing span's left.
    #[test]
    fn a_self_parenting_span_is_numbered_before_an_unrelated_root() {
        let spans = vec![
            SpanNode {
                span_id: sid(1),
                parent_span_id: Some(sid(1)),
            },
            SpanNode {
                span_id: sid(2),
                parent_span_id: None,
            },
        ];

        let sets = assign_nested_set(&spans);
        // Both are roots, in span order: the self-parent is first.
        assert2::check!(
            (sets[0].nested_set_left, sets[0].nested_set_right) == (1, 2),
            "self-parent: {:?}",
            sets[0]
        );
        assert2::check!(
            (sets[1].nested_set_left, sets[1].nested_set_right) == (3, 4),
            "unrelated root: {:?}",
            sets[1]
        );
        assert2::check!((sets[0].parent_id, sets[1].parent_id) == (-1, -1));
    }

    fn node(id: u8, parent: Option<u8>) -> SpanNode {
        SpanNode {
            span_id: sid(id),
            parent_span_id: parent.map(sid),
        }
    }

    fn sample_tree() -> Vec<SpanNode> {
        vec![
            node(1, None),
            node(2, Some(1)),
            node(3, Some(1)),
            node(4, Some(3)),
        ]
    }

    fn idx(spans: &[SpanNode], id: u8) -> usize {
        spans.iter().position(|s| s.span_id == sid(id)).unwrap()
    }

    #[test]
    fn root_has_sentinel_parent_id() {
        let spans = sample_tree();
        let ns = assign_nested_set(&spans);
        // -1 = Tempo's no-parent sentinel (so `nestedSetParent < 0` finds roots).
        assert2::assert!(ns[idx(&spans, 1)].parent_id == -1);
    }

    #[test]
    fn child_parent_id_equals_parent_left() {
        let spans = sample_tree();
        let ns = assign_nested_set(&spans);
        let p_left = ns[idx(&spans, 3)].nested_set_left;
        let root_left = ns[idx(&spans, 1)].nested_set_left;
        assert2::assert!(ns[idx(&spans, 4)].parent_id == p_left);
        assert2::assert!(ns[idx(&spans, 2)].parent_id == root_left);
        assert2::assert!(ns[idx(&spans, 3)].parent_id == root_left);
    }

    #[test]
    fn ancestor_interval_strictly_contains_descendants() {
        let spans = sample_tree();
        let ns = assign_nested_set(&spans);
        let r = ns[idx(&spans, 1)];
        for id in [2_u8, 3, 4] {
            let d = ns[idx(&spans, id)];
            assert2::assert!(r.nested_set_left < d.nested_set_left);
            assert2::assert!(d.nested_set_right < r.nested_set_right);
        }

        let three = ns[idx(&spans, 3)];
        let two = ns[idx(&spans, 2)];
        let four = ns[idx(&spans, 4)];
        assert2::assert!(
            three.nested_set_left < four.nested_set_left
                && four.nested_set_right < three.nested_set_right
        );
        assert2::assert!(
            !(two.nested_set_left < four.nested_set_left
                && four.nested_set_right < two.nested_set_right)
        );
    }

    #[test]
    fn orphan_is_treated_as_root() {
        let spans = vec![node(5, Some(99))];
        let ns = assign_nested_set(&spans);
        assert2::assert!(ns[0].parent_id == -1); // dangling parent → root sentinel
        assert2::assert!(ns[0].nested_set_left < ns[0].nested_set_right);
    }

    #[test]
    fn left_lt_right_for_every_node() {
        let spans = sample_tree();
        let ns = assign_nested_set(&spans);
        for n in &ns {
            assert2::assert!(n.nested_set_left < n.nested_set_right);
        }
    }

    #[test]
    fn cyclic_parentage_assigns_every_node_a_valid_interval() {
        for (_name, spans) in [
            ("two-node cycle", vec![node(1, Some(2)), node(2, Some(1))]),
            (
                "three-node cycle",
                vec![node(1, Some(3)), node(2, Some(1)), node(3, Some(2))],
            ),
        ] {
            let ns = assign_nested_set(&spans);
            let mut lefts: Vec<i32> = ns.iter().map(|n| n.nested_set_left).collect();
            lefts.sort_unstable();
            lefts.dedup();
            assert2::assert!(ns.iter().all(|n| n.nested_set_left < n.nested_set_right));
            assert2::assert!(lefts.len() == ns.len());
            assert2::assert!(ns.iter().any(|n| n.parent_id == -1));
        }
    }

    #[test]
    fn cyclic_parentage_assignment_completes() {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let spans = vec![node(1, None), node(2, Some(3)), node(3, Some(2))];
            let _ = tx.send(assign_nested_set(&spans));
        });

        let ns = rx
            .recv_timeout(Duration::from_millis(250))
            .expect("cyclic parentage assignment should complete");
        assert2::assert!(ns.len() == 3);
        assert2::assert!(ns.iter().all(|node| node.nested_set_left > 0));
    }
}

// === split-modules: generated submodules ===
mod assign_nested_set;
mod nested_set_type;
mod span_node;

pub use assign_nested_set::assign_nested_set;
pub use nested_set_type::NestedSet;
pub use span_node::SpanNode;
