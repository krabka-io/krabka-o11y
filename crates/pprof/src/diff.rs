//! Diff flamegraph alignment.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::{
    FlameGraphDiff, Level,
    tree::{Tree, TreeSnapshotNode, name_slot},
};

#[derive(Clone, Debug)]
pub(crate) struct Bar {
    pub(crate) node: Option<usize>,
    pub(crate) name: String,
    pub(crate) total_left: i64,
    pub(crate) self_left: i64,
    pub(crate) total_right: i64,
    pub(crate) self_right: i64,
    pub(crate) x_left: i64,
    pub(crate) x_right: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct MergedNode {
    pub(crate) name: String,
    pub(crate) total_left: i64,
    pub(crate) self_left: i64,
    pub(crate) total_right: i64,
    pub(crate) self_right: i64,
    pub(crate) children: Vec<usize>,
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::{Frame, Tree};

    struct DiffBarExpectation {
        index: usize,
        value: i64,
    }

    fn frame(name: &str) -> Frame {
        Frame {
            function: name.to_string(),
            file: String::new(),
            line: 0,
        }
    }

    #[test]
    fn diff_aligns_right_only_frame_with_zero_left() {
        let mut left = Tree::new();
        left.add_stack(&[frame("a")], 10);
        let mut right = Tree::new();
        right.add_stack(&[frame("a")], 10);
        right.add_stack(&[frame("b")], 5);

        let diff = diff_trees(&left, &right, 0);
        assert!(diff.left_ticks == 10);
        assert!(diff.right_ticks == 15);
        for level in &diff.levels {
            assert!(level.values.len() % 7 == 0);
        }

        let b_idx = i64::try_from(diff.names.iter().position(|name| name == "b").unwrap()).unwrap();
        let level1 = &diff.levels[1].values;
        let b_bar = level1.chunks(7).find(|chunk| chunk[6] == b_idx).unwrap();
        for expected in [
            DiffBarExpectation { index: 1, value: 0 },
            DiffBarExpectation { index: 2, value: 0 },
            DiffBarExpectation { index: 4, value: 5 },
            DiffBarExpectation { index: 5, value: 5 },
        ] {
            assert!(
                b_bar[expected.index] == expected.value,
                "b_bar[{}]",
                expected.index
            );
        }
    }

    #[test]
    fn diff_root_is_total_on_both_sides() {
        let mut left = Tree::new();
        left.add_stack(&[frame("a")], 3);
        let mut right = Tree::new();
        right.add_stack(&[frame("a")], 9);

        let diff = diff_trees(&left, &right, 0);
        let root = &diff.levels[0].values;
        assert!(root[1] == 3 && root[4] == 9);
        assert!(diff.names[usize::try_from(root[6]).unwrap()] == "total");
    }

    #[test]
    fn diff_truncation_keeps_hot_path_and_aggregates_hidden_subtrees() {
        let mut left = Tree::new();
        left.add_stack(&[frame("leaf_a"), frame("a_hidden")], 2);
        left.add_stack(&[frame("hot_leaf"), frame("m_parent")], 10);
        left.add_stack(&[frame("leaf_z"), frame("z_hidden")], 3);
        let mut right = Tree::new();
        right.add_stack(&[frame("hot_leaf"), frame("m_parent")], 8);
        right.add_stack(&[frame("leaf_z"), frame("z_hidden")], 4);

        let diff = diff_trees(&left, &right, 3);

        assert!(diff.left_ticks == 15);
        assert!(diff.right_ticks == 12);
        let parent = name_index(&diff, "m_parent");
        let leaf = name_index(&diff, "hot_leaf");
        let other = name_index(&diff, "other");
        assert!(diff.levels[1].values == vec![2, 10, 0, 0, 8, 0, parent, -2, 5, 5, 0, 4, 4, other]);
        assert!(diff.levels[2].values == vec![2, 10, 10, 0, 8, 8, leaf]);
    }

    #[test]
    fn diff_truncation_ranks_by_sum_and_emits_right_only_other() {
        let mut left = Tree::new();
        left.add_stack(&[frame("left_only")], 9);
        left.add_stack(&[frame("balanced")], 3);
        let mut right = Tree::new();
        right.add_stack(&[frame("balanced")], 3);

        let diff = diff_trees(&left, &right, 2);

        let left_only = name_index(&diff, "left_only");
        let other = name_index(&diff, "other");
        assert!(
            diff.levels[1].values == vec![3, 9, 9, 3, 0, 0, left_only, -3, 3, 3, -3, 3, 3, other]
        );

        let mut left = Tree::new();
        left.add_stack(&[frame("kept")], 10);
        let mut right = Tree::new();
        right.add_stack(&[frame("right_only")], 4);

        let diff = diff_trees(&left, &right, 2);

        let kept = name_index(&diff, "kept");
        let other = name_index(&diff, "other");
        assert!(diff.levels[1].values == vec![0, 10, 10, 0, 0, 0, kept, 0, 0, 0, 0, 4, 4, other]);
    }

    #[test]
    fn diff_level_offsets_accumulate_right_totals_between_kept_siblings() {
        let mut left = Tree::new();
        left.add_stack(&[frame("a")], 2);
        left.add_stack(&[frame("b")], 3);
        let mut right = Tree::new();
        right.add_stack(&[frame("a")], 2);
        right.add_stack(&[frame("b")], 3);

        let diff = diff_trees(&left, &right, 0);

        let a = name_index(&diff, "a");
        let b = name_index(&diff, "b");
        assert!(diff.levels[1].values == vec![0, 2, 2, 0, 2, 2, a, 0, 3, 3, 0, 3, 3, b]);
    }

    fn name_index(diff: &FlameGraphDiff, name: &str) -> i64 {
        i64::try_from(diff.names.iter().position(|value| value == name).unwrap()).unwrap()
    }
}

mod append_children;
mod diff_trees;
mod keep_set;
mod merge_node;

use append_children::append_children;
pub use diff_trees::diff_trees;
use keep_set::keep_set;
use merge_node::merge_node;

pub(crate) const OTHER_NAME: &str = "other";

pub(crate) const ROOT_NAME: &str = "total";

pub(crate) fn combined_total(nodes: &[MergedNode], node: usize) -> i64 {
    nodes[node].total_left + nodes[node].total_right
}

pub(crate) fn parents(nodes: &[MergedNode]) -> Vec<Option<usize>> {
    let mut parents = vec![None; nodes.len()];
    for (parent, node) in nodes.iter().enumerate() {
        for child in &node.children {
            parents[*child] = Some(parent);
        }
    }
    parents
}

pub(crate) fn subtree_self_left(nodes: &[MergedNode], node: usize) -> i64 {
    nodes[node].self_left
        + nodes[node]
            .children
            .iter()
            .map(|child| subtree_self_left(nodes, *child))
            .sum::<i64>()
}

pub(crate) fn subtree_self_right(nodes: &[MergedNode], node: usize) -> i64 {
    nodes[node].self_right
        + nodes[node]
            .children
            .iter()
            .map(|child| subtree_self_right(nodes, *child))
            .sum::<i64>()
}

pub(crate) fn children_by_name(
    node: Option<usize>,
    nodes: &[TreeSnapshotNode],
) -> BTreeMap<String, usize> {
    let mut children = BTreeMap::new();
    if let Some(index) = node {
        for child in &nodes[index].children {
            children.insert(nodes[*child].name.clone(), *child);
        }
    }
    children
}
