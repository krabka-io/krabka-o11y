use super::Tree;

pub(crate) fn subtree_self(tree: &Tree, node: usize) -> i64 {
    tree.nodes[node].self_
        + tree.nodes[node]
            .children
            .iter()
            .map(|child| subtree_self(tree, *child))
            .sum::<i64>()
}
