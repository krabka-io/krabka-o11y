use super::MergedNode;

pub(crate) fn subtree_self_right(nodes: &[MergedNode], node: usize) -> i64 {
    nodes[node].self_right
        + nodes[node]
            .children
            .iter()
            .map(|child| subtree_self_right(nodes, *child))
            .sum::<i64>()
}
