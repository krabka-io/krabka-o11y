//! Symbolized profile tree and flamegraph encoding.

use std::collections::{HashMap, HashSet};

use crate::Frame;

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::Frame;

    fn frame(name: &str) -> Frame {
        Frame {
            function: name.to_string(),
            file: String::new(),
            line: 0,
        }
    }

    fn stack(names: &[&str]) -> Vec<Frame> {
        names.iter().map(|name| frame(name)).collect()
    }

    #[test]
    fn add_stack_totals_along_path_and_self_at_leaf() {
        let mut tree = Tree::new();
        tree.add_stack(&stack(&["work", "main"]), 10);
        tree.add_stack(&stack(&["other", "main"]), 3);

        check!(tree.total_of(&["total"]) == 13);
        check!(tree.self_of(&["total"]) == 0);
        check!(tree.total_of(&["total", "main"]) == 13);
        check!(tree.self_of(&["total", "main"]) == 0);
        check!(tree.total_of(&["total", "main", "work"]) == 10);
        check!(tree.self_of(&["total", "main", "work"]) == 10);
        check!(tree.self_of(&["total", "main", "other"]) == 3);
    }

    #[test]
    fn add_stack_ignores_stackless_samples() {
        // Pyroscope keeps stackless samples in series sums but excludes them
        // from flamegraph totals; a stackless sample must not inflate the tree.
        let mut tree = Tree::new();
        tree.add_stack(&stack(&["work", "main"]), 10);
        tree.add_stack(&[], 1);

        assert!(tree.total_of(&["total"]) == 10);
        assert!(tree.self_of(&["total"]) == 0);
        let fg = tree.to_flamegraph(2048);
        assert!(fg.total == 10);
    }

    #[test]
    fn merge_combines_partial_trees() {
        let mut a = Tree::new();
        a.add_stack(&stack(&["work", "main"]), 10);
        let mut b = Tree::new();
        b.add_stack(&stack(&["work", "main"]), 5);
        b.add_stack(&stack(&["new", "main"]), 2);
        a.merge(&b);
        check!(a.total_of(&["total"]) == 17);
        check!(a.total_of(&["total", "main", "work"]) == 15);
        check!(a.self_of(&["total", "main", "new"]) == 2);
    }

    #[test]
    fn to_flamegraph_root_level_and_names() {
        let tree = sibling_tree();
        let fg = tree.to_flamegraph(2048);
        check!(fg.names[0] == "total");
        check!(fg.total == 10);
        check!(fg.levels[0].values == vec![0, 10, 0, 0]);
    }

    #[test]
    fn to_flamegraph_xoffset_is_delta_from_previous_bar_end() {
        let tree = sibling_tree();
        let fg = tree.to_flamegraph(2048);
        assert!(fg.levels[1].values[0..4] == [0, 10, 0, names_index(&fg, "main")]);
        let a = &fg.levels[2].values[0..4];
        assert!(a[0] == 0 && a[1] == 6 && a[2] == 6);
        let b = &fg.levels[2].values[4..8];
        assert!(b[0] == 0 && b[1] == 4 && b[2] == 4);
    }

    fn sibling_tree() -> Tree {
        let mut tree = Tree::new();
        tree.add_stack(&stack(&["a", "main"]), 6);
        tree.add_stack(&stack(&["b", "main"]), 4);
        tree
    }

    #[test]
    fn to_flamegraph_places_children_after_parent_self() {
        let mut tree = Tree::new();
        tree.add_stack(&stack(&["main"]), 5);
        tree.add_stack(&stack(&["work", "main"]), 7);

        let fg = tree.to_flamegraph(2048);
        let work = &fg.levels[2].values[0..4];

        assert!(work == [5, 7, 7, names_index(&fg, "work")]);
    }

    #[test]
    fn to_flamegraph_sorts_siblings_like_pyroscope_function_tree() {
        let mut tree = Tree::new();
        tree.add_stack(&stack(&["z_leaf", "main"]), 6);
        tree.add_stack(&stack(&["a_leaf", "main"]), 4);

        let fg = tree.to_flamegraph(2048);

        assert!(fg.names == vec!["total", "main", "z_leaf", "a_leaf"]);
        assert!(
            fg.levels[2].values
                == vec![
                    0,
                    4,
                    4,
                    names_index(&fg, "a_leaf"),
                    0,
                    6,
                    6,
                    names_index(&fg, "z_leaf"),
                ]
        );
    }

    #[test]
    fn to_flamegraph_truncates_with_synthetic_other() {
        let mut tree = Tree::new();
        for idx in 0..10 {
            tree.add_stack(&stack(&[&format!("leaf{idx}"), "main"]), idx + 1);
        }
        let fg = tree.to_flamegraph(4);
        assert!(fg.names.iter().any(|name| name == "other"));
        assert!(fg.total == 55);
    }

    #[test]
    fn to_flamegraph_keeps_all_tied_cumulative_nodes() {
        let mut tree = Tree::new();
        tree.add_stack(&stack(&["hot", "main"]), 100);
        let fg = tree.to_flamegraph(2);
        assert!(fg.names.iter().any(|name| name == "hot"));
        assert!(!fg.names.iter().any(|name| name == "other"));
        assert!(fg.total == 100);
        assert!(fg.max_self == 100);
    }

    #[test]
    fn to_flamegraph_synthetic_other_keeps_hidden_self_sum() {
        let mut tree = Tree::new();
        tree.add_stack(&stack(&["hot", "main"]), 10);
        tree.add_stack(&stack(&["cold", "main"]), 5);
        tree.add_stack(&stack(&["warm", "main"]), 4);

        let fg = tree.to_flamegraph(3);
        let other = names_index(&fg, "other");
        let other_bar = fg.levels[2]
            .values
            .as_chunks::<4>()
            .0
            .iter()
            .find(|chunk| chunk[3] == other)
            .unwrap();

        assert!(other_bar[1] == 9);
        assert!(other_bar[2] == 9);
    }

    #[test]
    fn sample_paths_exclude_internal_zero_self_and_restore_sibling_paths() {
        let mut tree = Tree::new();
        tree.add_stack(&stack(&["a", "main"]), 7);
        tree.add_stack(&stack(&["b", "main"]), 3);

        let mut samples = tree.sample_paths(2048);
        samples.sort();

        assert!(
            samples
                == vec![
                    tree_type::SamplePath {
                        frames: vec!["main".to_string(), "a".to_string()],
                        self_value: 7,
                    },
                    tree_type::SamplePath {
                        frames: vec!["main".to_string(), "b".to_string()],
                        self_value: 3,
                    },
                ]
        );
    }

    #[test]
    fn to_pyroscope_tree_bytes_uses_virtual_root_and_function_nodes() {
        let mut tree = Tree::new();
        tree.add_stack(&stack(&["work", "main"]), 7);

        let bytes = tree.to_pyroscope_tree_bytes(2048);

        assert!(bytes == b"\x00\x00\x01\x04main\x00\x01\x04work\x07\x00");
    }

    #[test]
    fn write_uvarint_encodes_single_and_multi_byte_values() {
        let mut out = Vec::new();

        write_uvarint(&mut out, 0);
        write_uvarint(&mut out, 127);
        write_uvarint(&mut out, 128);
        write_uvarint(&mut out, 300);

        assert!(out == vec![0x00, 0x7f, 0x80, 0x01, 0xac, 0x02]);
    }

    fn names_index(fg: &FlameGraph, name: &str) -> i64 {
        i64::try_from(fg.names.iter().position(|n| n == name).unwrap()).unwrap()
    }
}

mod append_children;
mod tree_type;

use append_children::append_children;
pub use tree_type::Tree;

#[derive(Clone, Debug)]
pub(crate) struct Bar {
    pub(crate) node: Option<usize>,
    pub(crate) name: String,
    pub(crate) total: i64,
    pub(crate) self_: i64,
    pub(crate) x_start: i64,
    pub(crate) level: usize,
}

/// Pyroscope-compatible single flamegraph projection.
#[derive(Clone, Debug, PartialEq)]
pub struct FlameGraph {
    pub names: Vec<String>,
    pub levels: Vec<Level>,
    pub total: i64,
    pub max_self: i64,
}

/// Diff flamegraph projection.
#[derive(Clone, Debug, PartialEq)]
pub struct FlameGraphDiff {
    pub names: Vec<String>,
    pub levels: Vec<Level>,
    pub left_ticks: i64,
    pub right_ticks: i64,
}

/// One flamegraph level, encoded as `[xOffsetDelta, total, self, nameIndex]` groups.
#[derive(Clone, Debug, PartialEq)]
pub struct Level {
    pub values: Vec<i64>,
}

#[derive(Clone, Debug)]
pub(crate) struct Node {
    pub(crate) name: String,
    pub(crate) total: i64,
    pub(crate) self_: i64,
    pub(crate) children: Vec<usize>,
    pub(crate) child_by_name: HashMap<String, usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TreeSnapshotNode {
    pub name: String,
    pub total: i64,
    pub self_: i64,
    pub children: Vec<usize>,
}

pub(crate) const OTHER_NAME: &str = "other";

pub(crate) const ROOT_NAME: &str = "total";

pub(crate) fn sorted_child_position(children: &[usize], nodes: &[Node], name: &str) -> usize {
    children
        .binary_search_by(|candidate| nodes[*candidate].name.as_str().cmp(name))
        .unwrap_or_else(|pos| pos)
}

pub(crate) fn subtree_self(tree: &Tree, node: usize) -> i64 {
    tree.nodes[node].self_
        + tree.nodes[node]
            .children
            .iter()
            .map(|child| subtree_self(tree, *child))
            .sum::<i64>()
}

pub(crate) fn write_pyroscope_tree_node(out: &mut Vec<u8>, name: &str, self_: i64) {
    write_uvarint(out, name.len() as u64);
    out.extend_from_slice(name.as_bytes());
    write_uvarint(out, self_.cast_unsigned());
}

pub(crate) fn write_uvarint(out: &mut Vec<u8>, mut value: u64) {
    for _ in 0..10 {
        if value < 0x80 {
            out.push(u8::try_from(value).expect("terminal uvarint byte fits in u8"));
            return;
        }
        let low_bits = u8::try_from(value & 0x7f).expect("masked uvarint byte fits in u8");
        out.push(low_bits + 0x80);
        value >>= 7;
    }
    unreachable!("u64 uvarint uses at most 10 bytes");
}

pub(crate) fn name_slot(
    names: &mut Vec<String>,
    index: &mut HashMap<String, i64>,
    name: &str,
) -> i64 {
    if let Some(slot) = index.get(name) {
        return *slot;
    }
    let slot = i64::try_from(names.len()).expect("name index fits i64");
    names.push(name.to_string());
    index.insert(name.to_string(), slot);
    slot
}
