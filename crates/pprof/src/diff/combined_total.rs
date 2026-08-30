use super::MergedNode;

pub(crate) fn combined_total(nodes: &[MergedNode], node: usize) -> i64 {
    nodes[node].total_left + nodes[node].total_right
}
