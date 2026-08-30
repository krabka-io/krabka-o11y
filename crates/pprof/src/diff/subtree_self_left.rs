use super::MergedNode;

pub(crate) fn subtree_self_left(nodes: &[MergedNode], node: usize) -> i64 {
    nodes[node].self_left
        + nodes[node]
            .children
            .iter()
            .map(|child| subtree_self_left(nodes, *child))
            .sum::<i64>()
}
