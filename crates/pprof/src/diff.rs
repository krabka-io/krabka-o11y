//! Diff flamegraph alignment.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::{
    FlameGraphDiff, Level,
    tree::{Tree, TreeSnapshotNode},
};

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::{Frame, Tree};

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
        for (index, want) in [(1, 0), (2, 0), (4, 5), (5, 5)] {
            assert!(b_bar[index] == want, "b_bar[{index}]");
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
mod bar;
mod children_by_name;
mod combined_total;
mod diff_trees;
mod keep_set;
mod merge_node;
mod merged_node;
mod name_slot;
mod other_name;
mod parents;
mod root_name;
mod subtree_self_left;
mod subtree_self_right;

use append_children::append_children;
use bar::Bar;
use children_by_name::children_by_name;
use combined_total::combined_total;
pub use diff_trees::diff_trees;
use keep_set::keep_set;
use merge_node::merge_node;
use merged_node::MergedNode;
use name_slot::name_slot;
use other_name::OTHER_NAME;
use parents::parents;
use root_name::ROOT_NAME;
use subtree_self_left::subtree_self_left;
use subtree_self_right::subtree_self_right;
